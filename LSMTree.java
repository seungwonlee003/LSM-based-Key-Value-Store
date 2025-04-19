public class LSMTree {
    private final MemtableService memtableSvc;
    private final SSTableService sstableSvc;
    private final CompactionService compactionSvc;

    public LSMTree(Config config) {
        this.memtableSvc = new MemtableService(config);
        this.sstableSvc = new SSTableService(config);
        this.compactionSvc = new CompactionService(sstableSvc, config);
    }

    public void startBackgroundTasks() {
        compactionSvc.start();
    }

    public void shutdown() {
        compactionSvc.stop();
        sstableSvc.close();
    }
    
    public String get(String key) {
        String value = memtableSvc.get(key);
        if(value != null) return value;
        return sstableSvc.get(key);
    }

    public void put(String key, String value) {
        memtableSvc.put(key, value);
        if (memtableSvc.shouldFlush()) {
            sstableSvc.flush(memtableSvc.switchMemtable());
        }
    }

    public void delete(String key) {
        memtableSvc.delete(key);
        if (memtableSvc.shouldFlush()) {
            sstableSvc.flush(memtableSvc.switchMemtable());
        }
    }
}
