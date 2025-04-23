import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CompactionService {

    private final MemtableService memtableService;
    private final Manifest manifest;
    private final Config config;
    private final ScheduledExecutorService memtableFlusher;
    private final ScheduledExecutorService compactionRunner;

    public CompactionService(MemtableService memtableService,
                            Manifest manifest,
                            Config config) {
        this.memtableService = Objects.requireNonNull(memtableService);
        this.manifest = Objects.requireNonNull(manifest);
        this.config = Objects.requireNonNull(config);

        memtableFlusher = Executors.newSingleThreadScheduledExecutor();
        memtableFlusher.scheduleAtFixedRate(
                this::flushMemtables,
                0,
                50,
                TimeUnit.MILLISECONDS
        );

        compactionRunner = Executors.newSingleThreadScheduledExecutor();
        compactionRunner.scheduleAtFixedRate(
                this::runCompaction,
                0,
                200,
                TimeUnit.MILLISECONDS
        );
    }

    // Flushes memtables to SSTables, minimizing read blocking. Reads are only blocked during flushQueue removal
    // and manifest updates to prevent inconsistent reads.
    private void flushMemtables() {
        memtableService.getLock().readLock().lock();
        try {
            if (!memtableService.hasFlushableMemtable()) {
                return;
            }
            // Select memtable without removing it to allow concurrent reads
            Memtable mem = memtableService.peekFlushableMemtable();
            if (mem == null) {
                return;
            }
    
            SSTable sstable = SSTable.createSSTableFromMemtable(mem);
    
            // Modify flushQueue and manifest under write lock to ensure consistency
            memtableService.getLock().readLock().unlock();
            memtableService.getLock().writeLock().lock();
            try {
                memtableService.removeFlushableMemtable(mem); // Remove from flushQueue
                manifest.getLock().writeLock().lock();
                try {
                    manifest.addSSTable(0, sstable);
                } finally {
                    manifest.getLock().writeLock().unlock();
                }
            } finally {
                memtableService.getLock().writeLock().unlock();
            }
        } finally {
            if (memtableService.getLock().readLock().tryLock()) {
                memtableService.getLock().readLock().unlock();
            }
        }
    }
    
    // Runs compaction, minimizing read blocking. Reads are only blocked during manifest updates
    // to prevent inconsistent SSTable references.
    private void runCompaction() {
        int maxLevel = manifest.maxLevel();
    
        for (int level = 0; level <= maxLevel; level++) {
            manifest.getLock().readLock().lock();
            try {
                List<SSTable> currentLevelTables = manifest.getSSTables(level);
    
                if (currentLevelTables.size() <= config.getLevelThreshold(level)) {
                    continue;
                }
    
                List<SSTable> tablesToMerge = new ArrayList<>(currentLevelTables);
                int nextLevel = level + 1;
                List<SSTable> nextLevelTables = manifest.getSSTables(nextLevel);
                tablesToMerge.addAll(nextLevelTables);
    
                List<SSTable> newTables = SSTable.sortedRun("./data", tablesToMerge.toArray(new SSTable[0]));
    
                manifest.getLock().readLock().unlock();
    
                // Update manifest atomically under write lock to ensure consistent SSTable references
                manifest.getLock().writeLock().lock();
                try {
                    manifest.replace(level, newTables);
                    // Delete obsolete SSTables
                    for (SSTable table : tablesToMerge) {
                        table.delete();
                    }
                } finally {
                    manifest.getLock().writeLock().unlock();
                }
            } finally {
                if (manifest.getLock().readLock().tryLock()) {
                    manifest.getLock().readLock().unlock();
                }
            }
        }
    }
    
    public void stop() {
        memtableFlusher.shutdown();
        compactionRunner.shutdown();
    }
}
