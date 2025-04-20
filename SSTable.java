import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class SSTable {
    private final String filePath;
    private final BloomFilter bloomFilter;
    private final NavigableMap<String, Long> index; // sparse index
    private static final int INDEX_INTERVAL = 100;  // index every 100th key

    private SSTable(String filePath) {
        this.filePath = filePath;
        this.index = new TreeMap<>();
        this.bloomFilter = new BloomFilter(1000, 3);
        init();
    }

    private SSTable(String filePath, BloomFilter bloomFilter, NavigableMap<String, Long> index){
        this.filePath = filePath;
        this.bloomFilter = bloomFilter;
        this.index = index;
    }

    public void init() throws IOException {
        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            long offset = 0;
            int count = 0;

            while (file.getFilePointer() < file.length()) {
                int keyLength = file.readInt();
                byte[] keyBytes = new byte[keyLength];
                file.readFully(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);

                int valueLength = file.readInt();
                byte[] valueBytes = new byte[valueLength];
                if (valueLength > 0) {
                    file.readFully(valueBytes);
                }

                bloomFilter.add(key);

                if (count % INDEX_INTERVAL == 0) {
                    index.put(key, offset);
                }

                offset += 4 + keyLength + 4 + valueLength;
                count++;
            }
        }
    }

    public static SSTable createSSTableFromMemtable(Memtable memtable) throws IOException {
        String filePath = "./data/sstable_" + System.nanoTime() + ".sst";
        BloomFilter bloomFilter = new BloomFilter(1000, 3);
        TreeMap<String, Long> index = new TreeMap<>();
        long segmentSize = 64 * 1024 * 1024; // 64MB target

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            Iterator<Map.Entry<String, String>> entries = memtable.iterator();
            long offset = 0;
            int count = 0;

            while (entries.hasNext()) {
                Map.Entry<String, String> entry = entries.next();
                String key = entry.getKey();
                String value = entry.getValue(); // May be null (tombstone)
                bloomFilter.add(key);

                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = value != null ? value.getBytes(StandardCharsets.UTF_8) : new byte[0];
                file.writeInt(keyBytes.length);
                file.write(keyBytes);
                file.writeInt(valueBytes.length);
                file.write(valueBytes);

                if (count % INDEX_INTERVAL == 0) {
                    index.put(key, offset);
                }

                offset += 4 + keyBytes.length + 4 + valueBytes.length;
                count++;
            }
        }

        return new SSTable(filePath, bloomFilter, index);
    }
    
    public boolean mightContain(String key) {
        return bloomFilter.mightContain(key);
    }

    public String get(String key) {
        // check bloom filter
        if (!bloomFilter.mightContain(key)) {
            return null;
        }

        // sparse index to find largest key <= target key
        Map.Entry<String, Long> indexEntry = index.floorEntry(key);
        if (indexEntry == null) {
            return null;
        }
        long offset = indexEntry.getValue();

        // sequential read from offset until key is found or larger key encountered
        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            file.seek(offset);
            while (file.getFilePointer() < file.length()) {
                // Read key-value pair
                int keyLength = file.readInt();
                byte[] keyBytes = new byte[keyLength];
                file.readFully(keyBytes);
                String currentKey = new String(keyBytes, StandardCharsets.UTF_8);
                
                int valueLength = file.readInt();
                byte[] valueBytes = new byte[valueLength];
                if (valueLength > 0) {
                    file.readFully(valueBytes);
                }
                
                if (currentKey.equals(key)) {
                    return valueLength > 0 ? new String(valueBytes, StandardCharsets.UTF_8) : null; // Null for tombstone
                }
                if (currentKey.compareTo(key) > 0) {
                    return null; // Key not found (passed the possible position)
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

    public String getFilePath() {
        return filePath;
    }
}

