public class SSTableService {
    private final Manifest manifest;

    public SSTableService(Manifest manifest) {
        this.manifest = Objects.requireNonNull(manifest);
    }

    public String get(String key) {
        manifest.getLock().readLock().lock();
        try {
            for (int level = 0; level <= manifest.maxLevel(); level++) {
                for (SSTable sstable : manifest.getSSTables(level)) {
                    String value = sstable.get(key);
                    if (value != null) {
                        return value;
                    }
                }
            }
            return null;
        } finally {
            manifest.getLock().readLock().unlock();
        }
    }
}
