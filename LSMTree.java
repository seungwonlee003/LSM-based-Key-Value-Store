public class LSMTree {
    private final MemtableService memtableSvc;
    private final SSTableService sstableSvc;
    private final Manifest manifest;
    private final CompactionService compactionSvc;

    public LSMTree(Config config) {
        this.manifest = new Manifest(config);
        this.memtableSvc = new MemtableService(config);
        this.sstableSvc = new SSTableService(config);
        this.compactionSvc = new CompactionService(memTableSvc, sstableSvc, manifest, config);
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
