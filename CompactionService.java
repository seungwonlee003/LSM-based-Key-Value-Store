public class CompactionService {

    private final MemtableService memtableService;
    private final SSTable sstable;
    private final Manifest manifest;
    private final Config config;
    private final ScheduledExecutorService memtableFlusher;
    private final ScheduledExecutorService compactionRunner;
    
    public CompactionService(MemtableService memtableService,
                             SSTable sstable,
                             Manifest manifest,
                             Config config) {
        this.memtableService = Objects.requireNonNull(memtableService);
        this.sstable  = Objects.requireNonNull(sstable);
        this.manifest        = Objects.requireNonNull(manifest);
        this.config          = Objects.requireNonNull(config);

        memtableFlusher = newSingleThreadScheduledExecutor();
        memtableFlusher.scheduleAtFixedRate(
            this::flushMemtables,    // task method reference
            0,                       // initial delay
            50,                      // repeat interval
            TimeUnit.MILLISECONDS   // time unit
        );

        compactionRunner = newSingleThreadScheduledExecutor();
        compactionRunner.scheduleAtFixedRate(
            this::runCompaction,
            0,
            200,
            TimeUnit.MILLISECONDS
        );
    }
    
    private void flushMemtables() {
        if (!memtableService.hasFlushableMemtable()) {
            return;
        }

        memtableService.getLock().writeLock().lock();
        manifest.getLock().writeLock().lock();
        try {
            Memtable mem = memtableService.pollFlushableMemtable();
            if (mem == null) {
                return;
            }
            SSTable sstable = sstable.createSSTableFromMemtable(mem);
            manifest.addSSTable(0, sstable);
        } finally {
            manifest.getLock().writeLock().unlock();
            memtableService.getLock().writeLock().unlock();
        }
    }


    private void runCompaction() {
        manifest.getLock().writeLock().lock();
        try {
            int maxLevel = manifest.maxLevel();
            for (int level = 0; level <= maxLevel; level++) {
                List<SSTable> tables = manifest.getSSTables(level);
                int threshold = config.getLevelThreshold(level);
                if (tables.size() <= threshold) {
                    continue;
                }

                List<SSTable> nextLevel = manifest.getSSTables(level + 1);
                List<SSTable> inputs = new ArrayList<>(tables);
                inputs.addAll(nextLevel);

                long targetSize = config.getSegmentSize(); // fixed to 64MB
                List<SSTable> merged = Compactor.mergeAndSplit(inputs, targetSize);

                for (SSTable sstable : inputs) {
                    sstable.delete();
                }

                manifest.replace(level, tables, level + 1, merged);
            }
        } finally {
            manifest.getLock().writeLock().unlock();
        }
    }

    public void stop() {
        memtableFlusher.shutdown();
        compactionRunner.shutdown();
        try {
            if (!memtableFlusher.awaitTermination(5, TimeUnit.SECONDS)) {
                memtableFlusher.shutdownNow();
            }
            if (!compactionRunner.awaitTermination(5, TimeUnit.SECONDS)) {
                compactionRunner.shutdownNow();
            }
        } catch (InterruptedException e) {
            memtableFlusher.shutdownNow();
            compactionRunner.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
