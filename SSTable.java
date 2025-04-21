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

    private SSTable(String filePath, BloomFilter bloomFilter, NavigableMap<String, Long> index) {
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
        long segmentSize = config.getSegmentSize();

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

    public static List<SSTable> sortedRun(String dataDir, long sstMaxSize, int currentLevel, int l0TableCount, SSTable... tables) throws IOException {
        SSTableIterator[] iterators = Arrays.stream(tables)
                .map(SSTableIterator::new)
                .toArray(SSTableIterator[]::new);

        PriorityQueue<SSTableEntry> queue = new PriorityQueue<>((e1, e2) -> {
            int keyCompare = e1.key.compareTo(e2.key);
            if (keyCompare != 0) return keyCompare;
            if (currentLevel == 0) {
                boolean e1IsL0 = e1.iteratorIndex < l0TableCount;
                boolean e2IsL0 = e2.iteratorIndex < l0TableCount;
                if (e1IsL0 != e2IsL0) return e1IsL0 ? -1 : 1;
            }
            return Integer.compare(e2.iteratorIndex, e1.iteratorIndex);
        });
        
        for (int i = 0; i < iterators.length; i++) {
            if (iterators[i].hasNext()) {
                queue.offer(new SSTableEntry(iterators[i].next(), i));
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
                if (currentSize >= config.getSegmentSize()) {
                    newSSTables.add(createSSTableFromBuffer(dataDir, buffer));
                    buffer.clear();
                    currentSize = 0;
                }
            }
            if (iterators[entry.iteratorIndex].hasNext()) {
                queue.offer(new SSTableEntry(iterators[entry.iteratorIndex].next(), entry.iteratorIndex));
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
        TreeMap<String, Long> index = new TreeMap<>();
        long offset = 0;
        int count = 0;

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            for (Map.Entry<String, String> entry : buffer) {
                String key = entry.getKey();
                String value = entry.getValue();
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

    private static class SSTableIterator implements Iterator<Map.Entry<String, String>> {
        private final RandomAccessFile file;
        private boolean closed;

        public SSTableIterator(SSTable sstable) {
            this.file = new RandomAccessFile(sstable.filePath, "r");
            this.closed = false;
        }

        @Override
        public boolean hasNext() {
            return !closed && file.getFilePointer() < file.length();
        }

        @Override
        public Map.Entry<String, String> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
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
        }

        public void close() {
            if (!closed) {
                file.close();
                closed = true;
            }
        }
    }

    private static class SSTableEntry {
        final String key;
        final String value;
        final int iteratorIndex;

        SSTableEntry(Map.Entry<String, String> entry, int iteratorIndex) {
            this.key = entry.getKey();
            this.value = entry.getValue();
            this.iteratorIndex = iteratorIndex;
        }
    }
}
