public class DB {
    private final MemtableService memtableSvc;
    private final SSTableService sstableSvc;
    private final Manifest manifest;
    private final CompactionService compactionSvc;

    public DB(Config config) {
        this.manifest = new Manifest();
        this.memtableSvc = new MemtableService(config);
        this.sstableSvc = new SSTableService(manifest);
        this.compactionSvc = new CompactionService(memTableSvc, manifest, config);
    }
    
    public String get(String key) {
        String value = memtableSvc.get(key);
        if(value != null) return value;
        return sstableSvc.get(key);
    }

    public void put(String key, String value) {
        memtableSvc.put(key, value);
    }

    public void delete(String key) {
        memtableSvc.delete(key);
    }
}
