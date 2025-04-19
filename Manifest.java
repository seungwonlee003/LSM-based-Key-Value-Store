public class Manifest {
    private final Map<Integer, List<SSTable>> levelMap;
    private final ReadWriteLock rwLock;

    public Manifest() {
        this.levelMap = new HashMap<>();
        this.rwLock = new ReentrantReadWriteLock();
    }

    public ReadWriteLock getLock() {
        return rwLock;
    }

    public void addSSTable(int level, SSTable sstable) {
        rwLock.writeLock().lock();
        try {
            levelMap.computeIfAbsent(level, k -> new ArrayList<>()).add(sstable);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public List<SSTable> getSSTables(int level) {
        rwLock.readLock().lock();
        try {
            return new ArrayList<>(levelMap.getOrDefault(level, new ArrayList<>()));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public int maxLevel() {
        rwLock.readLock().lock();
        try {
            return levelMap.isEmpty() ? -1 : levelMap.keySet().stream().max(Integer::compare).orElse(-1);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void replace(int levelToClear, List<SSTable> oldTables, int targetLevel, List<SSTable> newTables) {
        rwLock.writeLock().lock();
        try {
            levelMap.remove(levelToClear); // Clear the source level
            levelMap.computeIfAbsent(targetLevel, k -> new ArrayList<>()).addAll(newTables);
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
