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
        long segmentSize = 64 * 1024 * 1024; // 64MB, from CompactionService config

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            Iterator<Map.Entry<String, String>> entries = memtable.iterator();
            long offset = 0;
            int count = 0;
            int indexInterval = 10; // Sparse index every ~10 entries (adjustable)

            while (entries.hasNext()) {
                Map.Entry<String, String> entry = entries.next();
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

                // Stop if segment size exceeded (simplified)
                if (offset >= segmentSize) {
                    break; // Real system would create new SSTable
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
}
