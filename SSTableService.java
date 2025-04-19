public class SSTableService {
    private final Manifest manifest;
    private final long baseSegmentSize;

    public SSTableService(Manifest manifest, long baseSegmentSize) {
        this.manifest = Objects.requireNonNull(manifest);
        this.baseSegmentSize = baseSegmentSize;
    }

    public void flush(Memtable memtable) {
        SSTable sstable = SSTable.createFromMemtable(memtable, baseSegmentSize);
        manifest.addSSTable(0, sstable);
    }

    public String get(String key) {
        for (int level = 0; level <= manifest.maxLevel(); level++) {
            for (SSTable sstable : manifest.getSSTables(level)) {
                if (sstable.mightContain(key)) {
                    String value = sstable.get(key);
                    if (value != null) {
                        return value;
                    }
                }
            }
        }
        return null;
    }
}
