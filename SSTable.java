import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class SSTable {
    private final String filePath;
    private final BloomFilter bloomFilter;
    private final Map<String, Long> index; // Sparse index: key → file offset

    private SSTable(String filePath, BloomFilter bloomFilter, Map<String, Long> index) {
        this.filePath = filePath;
        this.bloomFilter = bloomFilter;
        this.index = index;
    }

    public static SSTable createSSTableFromMemtable(Memtable memtable) {
        String filePath = "sstable_" + System.nanoTime() + ".sst";
        BloomFilter bloomFilter = new BloomFilter(1000, 3); // Adjustable: ~1000 items, 3 hashes
        Map<String, Long> index = new TreeMap<>();
        long segmentSize = 64 * 1024 * 1024; // 64MB, from Config.getSegmentSize()

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            SortedMap<String, String> entries = memtable.getSortedEntries();
            long offset = 0;
            int indexInterval = Math.max(1, entries.size() / 10); // Sparse index every ~10 entries
            int count = 0;

            for (Map.Entry<String, String> entry : entries.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue(); // May be null (tombstone)
                bloomFilter.add(key);

                // Serialize key-value pair
                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = value != null ? value.getBytes(StandardCharsets.UTF_8) : new byte[0];
                file.writeInt(keyBytes.length);
                file.write(keyBytes);
                file.writeInt(valueBytes.length);
                file.write(valueBytes);

                // Update sparse index
                if (count % indexInterval == 0) {
                    index.put(key, offset);
                }
                offset += 4 + keyBytes.length + 4 + valueBytes.length;
                count++;

                // Simplified: Assume one file per Memtable (can split if > segmentSize)
                if (offset >= segmentSize) {
                    break; // Truncate for simplicity; real system would create new SSTable
                }
            }

            // Write index at end
            try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file.getFD()))) {
                oos.writeObject(index);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create SSTable: " + filePath, e);
        }

        return new SSTable(filePath, bloomFilter, index);
    }

    public boolean mightContain(String key) {
        return bloomFilter.mightContain(key);
    }

    public String get(String key) {
        if (!mightContain(key)) {
            return null;
        }

        // Find closest index key <= key
        String indexKey = null;
        for (String k : index.keySet()) {
            if (k.compareTo(key) <= 0) {
                indexKey = k;
            } else {
                break;
            }
        }

        if (indexKey == null) {
            return null;
        }

        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            long offset = index.get(indexKey);
            file.seek(offset);

            // Scan sequentially from index point
            while (file.getFilePointer() < file.length()) {
                int keyLength = file.readInt();
                byte[] keyBytes = new byte[keyLength];
                file.readFully(keyBytes);
                String readKey = new String(keyBytes, StandardCharsets.UTF_8);

                int valueLength = file.readInt();
                byte[] valueBytes = new byte[valueLength];
                file.readFully(valueBytes);
                String value = valueLength > 0 ? new String(valueBytes, StandardCharsets.UTF_8) : null;

                if (readKey.equals(key)) {
                    return value; // Null for tombstone
                }
                if (readKey.compareTo(key) > 0) {
                    break; // Passed sorted position
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read SSTable: " + filePath, e);
        }

        return null;
    }

    public void delete() {
        File file = new File(filePath);
        if (file.exists()) {
            if (!file.delete()) {
                throw new RuntimeException("Failed to delete SSTable: " + filePath);
            }
        }
    }

    // Simple Bloom Filter implementation
    private static class BloomFilter {
        private final BitSet bitSet;
        private final int size;
        private final int hashCount;

        public BloomFilter(int expectedItems, int hashCount) {
            this.size = optimalSize(expectedItems);
            this.bitSet = new BitSet(size);
            this.hashCount = hashCount;
        }

        public void add(String key) {
            for (int i = 0; i < hashCount; i++) {
                bitSet.set(hash(key, i));
            }
        }

        public boolean mightContain(String key) {
            for (int i = 0; i < hashCount; i++) {
                if (!bitSet.get(hash(key, i))) {
                    return false;
                }
            }
            return true;
        }

        private int hash(String key, int seed) {
            int hash = 0;
            for (char c : key.toCharArray()) {
                hash = 31 * hash + c + seed;
            }
            return Math.abs(hash % size);
        }

        private static int optimalSize(int expectedItems) {
            return expectedItems * 10; // ~10 bits per item
        }
    }
}
