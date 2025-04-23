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
        Memtable mem = getFlushableMemtable();
        if (mem == null) {
            return;
        }
    
        SSTable sstable = createSSTableFromMemtable(mem);
        updateFlushQueueAndManifest(mem, sstable);
    }
    
    private Memtable getFlushableMemtable() {
        Lock readLock = memtableService.getLock().readLock();
        readLock.lock();
        try {
            if (!memtableService.hasFlushableMemtable()) {
                return null;
            }
            return memtableService.peekFlushableMemtable();
        } finally {
            readLock.unlock();
        }
    }
    
    private SSTable createSSTableFromMemtable(Memtable mem) {
        // No locks needed; this is a read-only operation on the memtable
        return SSTable.createSSTableFromMemtable(mem);
    }
    
    private void updateFlushQueueAndManifest(Memtable mem, SSTable sstable) {
        Lock writeLock = memtableService.getLock().writeLock();
        writeLock.lock();
        try {
            memtableService.removeFlushableMemtable(mem); // Modify flushQueue
            Lock manifestWriteLock = manifest.getLock().writeLock();
            manifestWriteLock.lock();
            try {
                manifest.addSSTable(0, sstable); // Update manifest
            } finally {
                manifestWriteLock.unlock();
            }
        } finally {
            writeLock.unlock();
        }
    }
    
    // Runs compaction, minimizing read blocking. Reads are only blocked during manifest updates
    // to prevent inconsistent SSTable references.
    private void runCompaction() {
        int maxLevel = manifest.maxLevel();
    
        for (int level = 0; level <= maxLevel; level++) {
            List<SSTable> tablesToCompact = getTablesToCompact(level);
            if (tablesToCompact == null) {
                continue;
            }
    
            List<SSTable> newTables = compactTables(tablesToCompact);
            updateManifest(level, tablesToCompact, newTables);
        }
    }
    
    private List<SSTable> getTablesToCompact(int level) {
        Lock readLock = manifest.getLock().readLock();
        readLock.lock();
        try {
            List<SSTable> currentLevelTables = manifest.getSSTables(level);
            if (currentLevelTables.size() <= config.getLevelThreshold(level)) {
                return null;
            }
    
            List<SSTable> tablesToMerge = new ArrayList<>(currentLevelTables);
            int nextLevel = level + 1;
            List<SSTable> nextLevelTables = manifest.getSSTables(nextLevel);
            tablesToMerge.addAll(nextLevelTables);
            return tablesToMerge;
        } finally {
            readLock.unlock();
        }
    }
    
    private List<SSTable> compactTables(List<SSTable> tablesToMerge) {
        // No locks needed; this is an independent operation
        return SSTable.sortedRun("./data", tablesToMerge.toArray(new SSTable[0]));
    }
    
    private void updateManifest(int level, List<SSTable> oldTables, List<SSTable> newTables) {
        Lock writeLock = manifest.getLock().writeLock();
        writeLock.lock();
        try {
            manifest.replace(level, newTables);
            for (SSTable table : oldTables) {
                table.delete();
            }
        } finally {
            writeLock.unlock();
        }
    }
    
    public void stop() {
        memtableFlusher.shutdown();
        compactionRunner.shutdown();
    }
}
