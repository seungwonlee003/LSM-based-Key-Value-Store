import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class SSTableIterator implements Iterator<Map.Entry<String, String>> {
    private final RandomAccessFile file;
    private boolean closed;

    public SSTableIterator(SSTable sstable) {
        try {
            this.file = new RandomAccessFile(sstable.getFilePath(), "r");
            this.closed = false;
        } catch (IOException e) {
            throw new RuntimeException("Failed to open SSTable file for iteration", e);
        }
    }

    @Override
    public boolean hasNext() {
        try {
            return !closed && file.getFilePointer() < file.length();
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
            int keyLength = file.readInt();
            byte[] keyBytes = new byte[keyLength];
            file.readFully(keyBytes);
            String key = new String(keyBytes, StandardCharsets.UTF_8);

            int valueLength = file.readInt();
            byte[] valueBytes = new byte[valueLength];
            if (valueLength > 0) {
                file.readFully(valueBytes);
            }
            String value = valueLength > 0 ? new String(valueBytes, StandardCharsets.UTF_8) : null;

            return new AbstractMap.SimpleEntry<>(key, value);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read next entry from SSTable", e);
        }
    }

    public void close() {
        if (!closed) {
            try {
                file.close();
                closed = true;
            } catch (IOException e) {
                throw new RuntimeException("Failed to close SSTableIterator", e);
            }
        }
    }
}
