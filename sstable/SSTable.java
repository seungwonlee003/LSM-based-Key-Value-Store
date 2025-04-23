public class SSTable {
    private final String filePath;
    private final BloomFilter bloomFilter;
    private final NavigableMap<String, Long> index; // sparse index
    private String minKey;
    private String maxKey;
    private static final int INDEX_INTERVAL = 100;  // index every 100th key

    private SSTable(String filePath) throws IOException {
        this.filePath = filePath;
        this.index = new TreeMap<>();
        this.bloomFilter = new BloomFilter(1000, 3);
        this.minKey = null;
        this.maxKey = null;
        init();
    }

    private SSTable(String filePath, BloomFilter bloomFilter, NavigableMap<String, Long> index, String minKey, String maxKey) {
        this.filePath = filePath;
        this.bloomFilter = bloomFilter;
        this.index = index;
        this.minKey = minKey;
        this.maxKey = maxKey;
    }

    public void init() throws IOException {
        RandomAccessFile file = new RandomAccessFile(filePath, "r");
        try {
            long offset = 0;
            int count = 0;
            String firstKey = null;
            String lastKey = null;

            while (file.getFilePointer() < file.length()) {
                int keyLength = file.readInt();
                byte[] keyBytes = new byte[keyLength];
                file.readFully(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);

                if (firstKey == null) {
                    firstKey = key;
                }
                lastKey = key;

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
            this.minKey = firstKey;
            this.maxKey = lastKey;
        } finally {
            file.close();
        }
    }

    public static SSTable createSSTableFromMemtable(Memtable memtable) throws IOException {
        String filePath = "./data/sstable_" + System.nanoTime() + ".sst";
        BloomFilter bloomFilter = new BloomFilter(1000, 3);
        TreeMap<String, Long> index = new TreeMap<>();
        long segmentSize = Config.getInstance().getSegmentSize();
        String minKey = null;
        String maxKey = null;

        RandomAccessFile file = new RandomAccessFile(filePath, "rw");
        try {
            Iterator<Map.Entry<String, String>> entries = memtable.iterator();
            long offset = 0;
            int count = 0;

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
        } finally {
            file.close();
        }

        return new SSTable(filePath, bloomFilter, index, minKey, maxKey);
    }

    public static List<SSTable> sortedRun(String dataDir, List<SSTable> tables) throws IOException {
        SSTableIterator[] iterators = new SSTableIterator[tables.length];
        for (int i = 0; i < tables.length; i++) {
            iterators[i] = new SSTableIterator(tables[i]);
        }

        PriorityQueue<SSTableEntry> queue = new PriorityQueue<SSTableEntry>(new Comparator<SSTableEntry>() {
            @Override
            public int compare(SSTableEntry e1, SSTableEntry e2) {
                int keyCompare = e1.key.compareTo(e2.key);
                if (keyCompare != 0) {
                    return keyCompare;
                }
                return Integer.compare(e1.iteratorIndex, e2.iteratorIndex);
            }
        });

        for (int i = 0; i < iterators.length; i++) {
            if (iterators[i].hasNext()) {
                SSTableEntry nextEntry = iterators[i].next();
                queue.offer(new SSTableEntry(nextEntry.key, nextEntry.value, i));
            }
        }

        List<SSTable> newSSTables = new ArrayList<SSTable>();
        List<Map.Entry<String, String>> buffer = new ArrayList<Map.Entry<String, String>>();
        long currentSize = 0;
        String lastKey = null;

        while (!queue.isEmpty()) {
            SSTableEntry entry = queue.poll();
            String key = entry.key;
            if (lastKey == null || !lastKey.equals(key)) {
                lastKey = key;
                String value = entry.value;
                buffer.add(new AbstractMap.SimpleEntry<String, String>(key, value));
                currentSize += 4 + key.getBytes(StandardCharsets.UTF_8).length +
                               4 + (value != null ? value.getBytes(StandardCharsets.UTF_8).length : 0);
                if (currentSize >= Config.getInstance().getSegmentSize()) {
                    newSSTables.add(createSSTableFromBuffer(dataDir, buffer));
                    buffer.clear();
                    currentSize = 0;
                }
            }
            int idx = entry.sstableNumber;
            if (iterators[idx].hasNext()) {
                SSTableEntry nextEntry = iterators[idx].next();
                queue.offer(new SSTableEntry(nextEntry.key, nextEntry.value, idx));
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
        TreeMap<String, Long> index = new TreeMap<String, Long>();
        String minKey = null;
        String maxKey = null;
        long offset = 0;
        int count = 0;

        RandomAccessFile file = new RandomAccessFile(filePath, "rw");
        try {
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
        } finally {
            file.close();
        }

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

        Map.Entry<String, Long> indexEntry = index.floorEntry(key);
        if (indexEntry == null) {
            return null;
        }
        long offset = indexEntry.getValue();

        try {
            RandomAccessFile file = new RandomAccessFile(filePath, "r");
            try {
                file.seek(offset);
                while (file.getFilePointer() < file.length()) {
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
                        return valueLength > 0 ? new String(valueBytes, StandardCharsets.UTF_8) : null;
                    }
                    if (currentKey.compareTo(key) > 0) {
                        return null;
                    }
                }
            } finally {
                file.close();
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

    public String getFilePath() {
        return filePath;
    }
}
