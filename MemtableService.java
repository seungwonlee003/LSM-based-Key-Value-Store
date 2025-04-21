class MemtableService {
    private final Config config;
    public final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private Memtable activeMemtable;
    private final Queue<Memtable> flushQueue = new ArrayDeque<>();

    public MemtableService(Config config) {
        this.config = config;
        this.activeMemtable = new Memtable();
    }

    public String get(String key) { 
        rwLock.readLock().lock();
        try{
            String v = activeMemtable.get(key);
            if (v != null) return v;

            for (Memtable m : flushQueue) {
                v = m.get(key);
                if (v != null) return v;
            }
            return null;
        } finally {
            rwLock.readLock().unlock();
        }
    }
    
    public void put(String k, String v) {
        rwLock.writeLock().lock();
        try{
            activeMemtable.put(key, value);
            if (activeMemtable.size() >= config.getMemtableThresholdBytes()) {
                rotateMemtable();
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }
    
    public void delete(String key) {
        rwLock.writeLock().lock();
        try {
            activeMemtable.put(key, null);
            if (activeMemtable.size() >= config.getMemtableThresholdBytes()) {
                rotateMemtable();
            }
        } finally {
            rwLock.writeLock().unlock();
        }    
    }

    private void rotateMemtable() {
        flushQueue.add(activeMemtable);
        activeMemtable = new Memtable();
    }
    
    public Memtable pollFlushableMemtable() {
        return flushQueue.poll();

    }

    public boolean hasFlushableMemtable() {
        return !flushQueue.isEmpty();
    }

    public ReadWriteLock getLock(){
        return rwLock;
    }
}
