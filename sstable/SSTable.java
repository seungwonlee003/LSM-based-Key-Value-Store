package sstable;

import memtable.Memtable;
import util.BloomFilter;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class SSTable {
    private final String filePath;
    public final BloomFilter bloomFilter;
    private final NavigableMap<String, BlockInfo> index;
    private static final int BLOCK_SIZE = 4096;
    private static final int SSTABLE_SIZE_THRESHOLD = 30;
    private String minKey;
    private String maxKey;

    static class BlockInfo {
        long offset;
        long length;

        BlockInfo(long offset, long length) {
            this.offset = offset;
            this.length = length;
        }
    }

    public SSTable(String filePath) throws IOException {
        this.filePath = filePath;
        this.index = new TreeMap<>();
        this.bloomFilter = new BloomFilter(1000, 3);
        this.minKey = null;
        this.maxKey = null;
        init();
    }

    public SSTable(String filePath, BloomFilter bloomFilter, TreeMap<String, BlockInfo> index, String minKey, String maxKey) {
        this.filePath = filePath;
        this.bloomFilter = bloomFilter;
        this.index = index;
        this.minKey = minKey;
        this.maxKey = maxKey;
    }
        
    public void init() throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filePath, "r");
             DataInputStream dataIn = new DataInputStream(
                 new BufferedInputStream(new FileInputStream(file.getFD()), BLOCK_SIZE))) {
            long currentOffset = 0;
            long blockStartOffset = 0;
            int currentBlockSize = 0;
            String firstKeyOfBlock = null;
    
            while (file.getFilePointer() < file.length()) {
                // Read key length
                int keyLength;
                try {
                    keyLength = dataIn.readInt();
                    if (keyLength < 0) {
                        throw new IOException("Invalid key length: " + keyLength);
                    }
                } catch (EOFException e) {
                    throw new IOException("Unexpected EOF while reading key length", e);
                }
    
                // Read key bytes
                byte[] keyBytes = new byte[keyLength];
                dataIn.readFully(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);
    
                // Read value length
                int valueLength;
                try {
                    valueLength = dataIn.readInt();
                    if (valueLength < 0) {
                        throw new IOException("Invalid value length: " + valueLength);
                    }
                } catch (EOFException e) {
                    throw new IOException("Unexpected EOF while reading value length", e);
                }
    
                // Read value bytes
                byte[] valueBytes = new byte[valueLength];
                if (valueLength > 0) {
                    dataIn.readFully(valueBytes);
                }
    
                // Calculate pair size
                int pairSize = 4 + keyLength + 4 + valueLength;
    
                // Start new block or finalize current block
                if (currentBlockSize == 0) {
                    firstKeyOfBlock = key;
                    blockStartOffset = currentOffset;
                } else if (currentBlockSize + pairSize > BLOCK_SIZE) {
                    long blockLength = currentOffset - blockStartOffset;
                    index.put(firstKeyOfBlock, new BlockInfo(blockStartOffset, blockLength));
                    firstKeyOfBlock = key;
                    blockStartOffset = currentOffset;
                    currentBlockSize = 0;
                }
    
                // Update offset, block size, and Bloom filter
                currentOffset += pairSize;
                currentBlockSize += pairSize;
                bloomFilter.add(key);
    
                // Update min/max keys
                if (minKey == null) {
                    minKey = key;
                }
                maxKey = key;
            }
    
            // Finalize last block if not empty
            if (currentBlockSize > 0) {
                long blockLength = currentOffset - blockStartOffset;
                index.put(firstKeyOfBlock, new BlockInfo(blockStartOffset, blockLength));
            }
        }
    }

    public static SSTable createSSTableFromMemtable(Memtable memtable) throws IOException {
            String filePath = "./data/sstable_" + System.nanoTime() + ".sst";
            BloomFilter bloomFilter = new BloomFilter(1000, 3);
            TreeMap<String, BlockInfo> index = new TreeMap<>();
            String minKey = null;
            String maxKey = null;
    
            try (RandomAccessFile file = new RandomAccessFile(filePath, "rw");
                 BufferedOutputStream bufferedOut = new BufferedOutputStream(new FileOutputStream(file.getFD()), BLOCK_SIZE);
                 DataOutputStream dataOut = new DataOutputStream(bufferedOut)) {
                Iterator<Map.Entry<String, String>> entries = memtable.iterator();
                long currentOffset = 0;
                long blockStartOffset = 0;
                int currentBlockSize = 0;
                String firstKeyOfBlock = null;
            
                while (entries.hasNext()) {
                    Map.Entry<String, String> entry = entries.next();
                    String key = entry.getKey();
                    String value = entry.getValue();
                    bloomFilter.add(key);
            
                    if (minKey == null) {
                        minKey = key;
                    }
                    maxKey = key;
            
                    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                    byte[] valueBytes = value != null ? value.getBytes(StandardCharsets.UTF_8) : new byte[0];
                    int pairSize = 4 + keyBytes.length + 4 + valueBytes.length;
            
                    if (currentBlockSize + pairSize > BLOCK_SIZE && currentBlockSize > 0) {
                        long blockLength = currentOffset - blockStartOffset;
                        index.put(firstKeyOfBlock, new BlockInfo(blockStartOffset, blockLength));
                        firstKeyOfBlock = key;
                        blockStartOffset = currentOffset;
                        currentBlockSize = 0;
                    }
                    if (currentBlockSize == 0) {
                        firstKeyOfBlock = key;
                    }
            
                    dataOut.writeInt(keyBytes.length);
                    dataOut.write(keyBytes);
                    dataOut.writeInt(valueBytes.length);
                    dataOut.write(valueBytes);
            
                    currentOffset += pairSize;
                    currentBlockSize += pairSize;
                }
    
                if (currentBlockSize > 0) {
                    long blockLength = currentOffset - blockStartOffset;
                    index.put(firstKeyOfBlock, new BlockInfo(blockStartOffset, blockLength));
                }
            }
    
            return new SSTable(filePath, bloomFilter, index, minKey, maxKey);
        }
    }

    public static List<SSTable> sortedRun(String dataDir, List<SSTable> tables) throws IOException {
        SSTableIterator[] iterators = new SSTableIterator[tables.size()];
        for (int i = 0; i < tables.size(); i++) {
            iterators[i] = new SSTableIterator(tables.get(i));
        }

        PriorityQueue<SSTableEntry> queue = new PriorityQueue<>(new Comparator<SSTableEntry>() {
            @Override
            public int compare(SSTableEntry e1, SSTableEntry e2) {
                int keyCompare = e1.key.compareTo(e2.key);
                if (keyCompare != 0) {
                    return keyCompare;
                }
                return Integer.compare(e1.sstableNumber, e2.sstableNumber);
            }
        });

        for (int i = 0; i < iterators.length; i++) {
            if (iterators[i].hasNext()) {
                Map.Entry<String, String> nextEntry = iterators[i].next();
                queue.offer(new SSTableEntry(nextEntry, i));
            }
        }

        List<SSTable> newSSTables = new ArrayList<>();
        List<Map.Entry<String, String>> buffer = new ArrayList<>();
        long currentSize = 0;
        String lastKey = null;

        while (!queue.isEmpty()) {
            SSTableEntry entry = queue.poll();
            String key = entry.key;
            if (lastKey == null || !lastKey.equals(key)) {
                lastKey = key;
                String value = entry.value;
                buffer.add(new AbstractMap.SimpleEntry<>(key, value));
                currentSize += 4 + key.getBytes(StandardCharsets.UTF_8).length +
                        4 + (value != null ? value.getBytes(StandardCharsets.UTF_8).length : 0);
                if (currentSize >= SSTABLE_SIZE_THRESHOLD) {
                    newSSTables.add(createSSTableFromBuffer(dataDir, buffer));
                    buffer.clear();
                    currentSize = 0;
                }
            }
            int idx = entry.sstableNumber;
            if (iterators[idx].hasNext()) {
                Map.Entry<String, String> nextEntry = iterators[idx].next();
                queue.offer(new SSTableEntry(nextEntry, idx));
            }
        }

        if (!buffer.isEmpty()) {
            newSSTables.add(createSSTableFromBuffer(dataDir, buffer));
        }

        for (SSTableIterator iterator : iterators) {
            iterator.close();
        }

        return newSSTables;
    }

    private static SSTable createSSTableFromBuffer(String dataDir, List<Map.Entry<String, String>> buffer) throws IOException {
        String filePath = dataDir + "/sstable_" + System.nanoTime() + ".sst";
        BloomFilter bloomFilter = new BloomFilter(1000, 3);
        TreeMap<String, BlockInfo> index = new TreeMap<>();
        String minKey = null;
        String maxKey = null;
    
        long currentOffset = 0L;
        long blockStartOffset = 0L;
        int currentBlockSize = 0;
        String firstKeyOfBlock = null;
    
        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw");
             BufferedOutputStream bufferedOut = new BufferedOutputStream(new FileOutputStream(file.getFD()), BLOCK_SIZE);
             DataOutputStream dataOut = new DataOutputStream(bufferedOut)) {
            for (Map.Entry<String, String> entry : buffer) {
                String key = entry.getKey();
                String value = entry.getValue();
        
                bloomFilter.add(key);
                if (minKey == null) {
                    minKey = key;
                }
                maxKey = key;
        
                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = value != null ? value.getBytes(StandardCharsets.UTF_8) : new byte[0];
                int pairSize = Integer.BYTES + keyBytes.length + Integer.BYTES + valueBytes.length;
        
                if (currentBlockSize + pairSize > BLOCK_SIZE && currentBlockSize > 0) {
                    long blockLength = currentOffset - blockStartOffset;
                    index.put(firstKeyOfBlock, new BlockInfo(blockStartOffset, blockLength));
                    firstKeyOfBlock = key;
                    blockStartOffset = currentOffset;
                    currentBlockSize = 0;
                }
        
                if (currentBlockSize == 0) {
                    firstKeyOfBlock = key;
                    blockStartOffset = currentOffset;
                }
        
                dataOut.writeInt(keyBytes.length);
                dataOut.write(keyBytes);
                dataOut.writeInt(valueBytes.length);
                dataOut.write(valueBytes);
        
                currentOffset += pairSize;
                currentBlockSize += pairSize;
            }
    
            if (currentBlockSize > 0) {
                long blockLength = currentOffset - blockStartOffset;
                index.put(firstKeyOfBlock, new BlockInfo(blockStartOffset, blockLength));
            }
        }
    
        // Return new SSTable with constructed metadata
        return new SSTable(filePath, bloomFilter, index, minKey, maxKey);
    }

    public boolean mightContain(String key) {
        return bloomFilter.mightContain(key);
    }

    public String get(String key) {
        if (key.compareTo(minKey) < 0 || key.compareTo(maxKey) > 0) {
            return null;
        }

        if (!bloomFilter.mightContain(key)) {
            return null;
        }

        Map.Entry<String, BlockInfo> floorEntry = index.floorEntry(key);
        if (floorEntry == null) {
            return null;
        }

        BlockInfo blockInfo = floorEntry.getValue();

        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            file.seek(blockInfo.offset);
            byte[] blockData = new byte[(int) blockInfo.length];
            file.readFully(blockData);

            try (ByteArrayInputStream bais = new ByteArrayInputStream(blockData);
                 DataInputStream dis = new DataInputStream(bais)) {
                while (dis.available() > 0) {
                    int keyLength = dis.readInt();
                    byte[] keyBytes = new byte[keyLength];
                    dis.readFully(keyBytes);
                    String currentKey = new String(keyBytes, StandardCharsets.UTF_8);

                    int valueLength = dis.readInt();
                    byte[] valueBytes = new byte[valueLength];
                    if (valueLength > 0) {
                        dis.readFully(valueBytes);
                    }

                    if (currentKey.equals(key)) {
                        return valueLength > 0 ? new String(valueBytes, StandardCharsets.UTF_8) : null;
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read SSTable: " + filePath, e);
        }

        return null;
    }

    public void delete() {
        File file = new File(filePath);
        if (file.exists() && !file.delete()) {
            throw new RuntimeException("Failed to delete SSTable: " + filePath);
        }
    }

    public NavigableMap<String, BlockInfo> getIndex() {
        return index;
    }

    public String getFilePath() {
        return filePath;
    }
}
