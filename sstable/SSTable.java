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
    private static final int SSTABLE_MAX_SIZE = 16 * 1080 * 1080;
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

    // Read SSTable file in 4KB chunks to rebuild index blocks and Bloom filter
    public void init() throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filePath, "r");
             DataInputStream dataIn = new DataInputStream(new BufferedInputStream(new FileInputStream(file.getFD()), BLOCK_SIZE))) {
            
            long currentOffset = 0;
            long blockStartOffset = 0;
            int currentBlockSize = 0;
            String firstKeyOfBlock = null;
    
            while (true) {
                try {
                    keyLength = dataIn.readInt();
                } catch (EOFException eof) {
                    break;  // end of file → exit loop
                }
                byte[] keyBytes = new byte[keyLength];
                dataIn.readFully(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);
    
                int valueLength = dataIn.readInt();
                byte[] valueBytes = new byte[valueLength];
                dataIn.readFully(valueBytes);
    
                int pairSize = 4 + keyLength + 4 + valueLength;

                if (currentBlockSize + pairSize > BLOCK_SIZE) {
                    long blockLength = currentOffset - blockStartOffset;
                    index.put(firstKeyOfBlock, new BlockInfo(blockStartOffset, blockLength));
                    firstKeyOfBlock = key;
                    blockStartOffset = currentOffset;
                    currentBlockSize = 0;
                }

                if (currentBlockSize == 0) {
                    firstKeyOfBlock = key;
                }

                currentOffset += pairSize;
                currentBlockSize += pairSize;
                bloomFilter.add(key);
    
                if (minKey == null) {
                    minKey = key;
                }
                maxKey = key;
            }
    
            if (currentBlockSize > 0) {
                long blockLength = currentOffset - blockStartOffset;
                index.put(firstKeyOfBlock, new BlockInfo(blockStartOffset, blockLength));
            }
        }
    }

    // Write memtable to SSTable in 4KB buffered chunks, indexing blocks of ~4KB
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

                // Keys and values are never null; if deleted, a special marker becomes the value.
                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
                
                int pairSize = 4 + keyBytes.length + 4 + valueBytes.length;
        
                if (currentBlockSize + pairSize > BLOCK_SIZE) {
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
                        
                if (minKey == null) {
                    minKey = key;
                }
                maxKey = key;
            }

            if (currentBlockSize > 0) {
                long blockLength = currentOffset - blockStartOffset;
                index.put(firstKeyOfBlock, new BlockInfo(blockStartOffset, blockLength));
            }
        }
        return new SSTable(filePath, bloomFilter, index, minKey, maxKey);
    }

    // Merge SSTables, writing pairs directly with 4KB I/O buffering, indexing logical 4KB blocks, and splitting at 16MB sstable threshold size
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
        String lastKey = null;

        // SSTable state
        String filePath = null;
        RandomAccessFile file = null;
        BufferedOutputStream bufferedOut = null;
        DataOutputStream dataOut = null;
        BloomFilter bloomFilter = null;
        TreeMap<String, BlockInfo> index = null;
        String minKey = null;
        String maxKey = null;

        // Index block variables
        long currentOffset = 0;
        long blockStartOffset = 0;
        int currentBlockSize = 0;
        String firstKeyOfBlock = null;
        
        while (!queue.isEmpty()) {
            SSTableEntry entry = queue.poll();
            String key = entry.key;

            if (lastKey == null || !lastKey.equals(key)) {
                lastKey = key;
                String value = entry.value;
                
                // Initialize SSTable if none exists
                if (file == null) {
                    filePath = "./data/sstable_" + System.nanoTime() + ".sst";
                    file = new RandomAccessFile(filePath, "rw");
                    bufferedOut = new BufferedOutputStream(new FileOutputStream(file.getFD()), BLOCK_SIZE);
                    dataOut = new DataOutputStream(bufferedOut);
                    bloomFilter = new BloomFilter(1000, 3);
                    index = new TreeMap<>();
                    minKey = null;
                    maxKey = null;
                    blockStartOffset = 0;
                    firstKeyOfBlock = key;
                }
                
                // Calculate pair size, handling null values
                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
                int pairSize = 4 + keyBytes.length + 4 + valueBytes.length;

                if (currentBlockSize + pairSize > BLOCK_SIZE) {
                    long blockLength = currentOffset - blockStartOffset;
                    index.put(firstKeyOfBlock, new BlockInfo(blockStartOffset, blockLength));
                    
                    blockStartOffset = currentOffset;
                    firstKeyOfBlock = key;
                    currentBlockSize = 0;
                }

                // Write pair directly
                dataOut.writeInt(keyBytes.length);
                dataOut.write(keyBytes);
                dataOut.writeInt(valueBytes.length);
                dataOut.write(valueBytes);

                // Update block and SSTable sizes
                currentBlockSize += pairSize;
                currentOffset += pairSize;
                
                // Update metadata
                bloomFilter.add(key);
                if (minKey == null) {
                    minKey = key;
                }
                maxKey = key;

                // Check if SSTable size exceeds max size
                if (currentOffset >= SSTABLE_MAX_SIZE) {
                    // Finalize last block
                    if (currentBlockSize > 0) {
                        long blockLength = currentOffset - blockStartOffset;
                        index.put(firstKeyOfBlock, new BlockInfo(blockStartOffset, blockLength));
                    }

                    // Close current SSTable
                    try {
                        dataOut.close();
                        file.close();
                    } catch (IOException e) {
                        throw new IOException("Failed to close SSTable: " + filePath, e);
                    }
                    newSSTables.add(new SSTable(filePath, bloomFilter, index, minKey, maxKey));

                    // Reset SSTable state
                    file = null;
                    bufferedOut = null;
                    dataOut = null;
                    bloomFilter = null;
                    index = null;
                    minKey = null;
                    maxKey = null;
                    firstKeyOfBlock = null;
                    blockStartOffset = 0;
                    currentBlockSize = 0;
                    currentOffset = 0;
                }
            }

            // Fetch next entry from iterator
            int idx = entry.sstableNumber;
            if (iterators[idx].hasNext()) {
                Map.Entry<String, String> nextEntry = iterators[idx].next();
                queue.offer(new SSTableEntry(nextEntry, idx));
            }
        }

        // Finalize any remaining data
        if (file != null && currentSSTableSize > 0) {
            // Finalize last block
            if (currentBlockSize > 0) {
                long blockLength = currentSSTableSize - blockStartOffset;
                index.put(firstKeyOfBlock, new BlockInfo(blockStartOffset, blockLength));
            }

            // Close and add final SSTable
            try {
                dataOut.close();
                file.close();
            } catch (IOException e) {
                throw new IOException("Failed to close SSTable: " + filePath, e);
            }
            newSSTables.add(new SSTable(filePath, bloomFilter, index, minKey, maxKey));
        }

        // Close all iterators
        for (SSTableIterator iterator : iterators) {
            iterator.close();
        }

        return newSSTables;
    }
        
    public boolean mightContain(String key) {
        return bloomFilter.mightContain(key);
    }

    // Read block (~4KB) from disk using block index to find key
    public String get(String key) {
        // Range check per sstable - returns if the key doesn't fall in the range
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
                    dis.readFully(valueBytes);

                    if (currentKey.equals(key)) {
                        return new String(valueBytes, StandardCharsets.UTF_8);
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
