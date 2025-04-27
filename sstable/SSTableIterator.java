import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class SSTableIterator implements Iterator<Map.Entry<String, String>> {
    private final RandomAccessFile file;
    private final Iterator<Map.Entry<String, SSTable.BlockInfo>> indexIterator;
    private final long fileLength;
    private boolean closed;
    private ByteArrayInputStream blockBuffer;
    private DataInputStream blockDataIn;
    private long currentBlockEnd;

    public SSTableIterator(SSTable sstable) {
        try {
            this.file = new RandomAccessFile(sstable.getFilePath(), "r");
            this.indexIterator = sstable.getIndex().entrySet().iterator();
            this.fileLength = file.length();
            this.closed = false;
            this.blockBuffer = null;
            this.blockDataIn = null;
            this.currentBlockEnd = 0;
        } catch (IOException e) {
            throw new RuntimeException("Failed to open SSTable file for iteration", e);
        }
    }

    @Override
    public boolean hasNext() {
        try {
            return !closed && (
                (blockDataIn != null && blockDataIn.available() > 0) ||
                indexIterator.hasNext() ||
                file.getFilePointer() < file.length()
            );
        } catch (IOException e) {
            throw new RuntimeException("Error checking file pointer", e);
        }
    }

    @Override
    public Map.Entry<String, String> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        try {
            if (blockDataIn == null || file.getFilePointer() >= currentBlockEnd) {
                loadNextBlock();
            }

            int keyLength = blockDataIn.readInt();
            byte[] keyBytes = new byte[keyLength];
            blockDataIn.readFully(keyBytes);
            String key = new String(keyBytes, StandardCharsets.UTF_8);

            int valueLength = blockDataIn.readInt();
            byte[] valueBytes = new byte[valueLength];
            blockDataIn.readFully(valueBytes);
            String value = new String(valueBytes, StandardCharsets.UTF_8);

            return new AbstractMap.SimpleEntry<>(key, value);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read next entry from SSTable", e);
        }
    }

    private void loadNextBlock() throws IOException {
        if (!indexIterator.hasNext()) {
            throw new IOException("No more blocks available in index");
        }

        Map.Entry<String, SSTable.BlockInfo> entry = indexIterator.next();
        SSTable.BlockInfo blockInfo = entry.getValue();

        file.seek(blockInfo.offset);

        byte[] blockData = new byte[(int) blockInfo.length];
        file.readFully(blockData);

        blockBuffer = new ByteArrayInputStream(blockData);
        blockDataIn = new DataInputStream(blockBuffer);

        currentBlockEnd = blockInfo.offset + blockInfo.length;
        file.seek(currentBlockEnd);
    }

    public void close() {
        if (!closed) {
            try {
                if (blockDataIn != null) {
                    blockDataIn.close();
                }
                file.close();
                closed = true;
            } catch (IOException e) {
                throw new RuntimeException("Failed to close SSTableIterator", e);
            }
        }
    }
}
