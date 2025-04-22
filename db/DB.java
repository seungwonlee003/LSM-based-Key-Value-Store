public class DB {
    private final MemtableService memtableService;
    private final SSTableService sstableService;
    private final Manifest manifest;
    private final CompactionService compactionService;

    public DB(Config config) {
        this.manifest = new Manifest();
        this.memtableService = new MemtableService(config);
        this.sstableService = new SSTableService(manifest);
        this.compactionService = new CompactionService(memtableService, manifest, config);
    }
    
    public String get(String key) {
        // first, search in mutable/immutable memtable
        String value = memtableService.get(key);
        if(value != null) return value;
        // if not found, search in sstable
        return sstableService.get(key);
    }

    public void put(String key, String value) {
        memtableService.put(key, value);
    }

    public void delete(String key) {
        memtableService.delete(key);
    }
}
