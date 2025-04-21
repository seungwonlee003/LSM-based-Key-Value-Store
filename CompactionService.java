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
            SSTable sstable = SSTable.createSSTableFromMemtable(mem);
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
                List<SSTable> currentLevelTables = manifest.getSSTables(level);

                if (currentLevelTables.size() <= config.getLevelThreshold(level)) {
                    continue;
                }

                List<SSTable> tablesToMerge = new ArrayList<>(currentLevelTables);
                int nextLevel = level + 1;
                List<SSTable> nextLevelTables = manifest.getSSTables(nextLevel);
                tablesToMerge.addAll(nextLevelTables);

                List<SSTable> newTables = SSTable.sortedRun("./data", sstMaxSize, level, currentLevelTables.size(), tablesToMerge.toArray(new SSTable[0]));

                manifest.replace(level, currentLevelTables, nextLevel, newTables);

                for (SSTable table : tablesToMerge) {
                    table.delete();
                }
            }
        } finally {
            manifest.getLock().writeLock().unlock();
        }
    }


    public void stop() {
        memtableFlusher.shutdown();
        compactionRunner.shutdown();
    }
}
