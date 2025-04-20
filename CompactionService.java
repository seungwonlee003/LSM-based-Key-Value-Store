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
        } catch (RuntimeException e) {
            // Log error and continue to ensure locks are released
            System.err.println("Failed to flush Memtable: " + e.getMessage());
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

                long targetSize = config.getSegmentSize(); // 64MB
                List<SSTable> merged = Compactor.mergeAndSplit(inputs, targetSize);

                for (SSTable sstable : inputs) {
                    try {
                        sstable.delete();
                    } catch (RuntimeException e) {
                        // Log error and continue
                        System.err.println("Failed to delete SSTable: " + e.getMessage());
                    }
                }

                manifest.replace(level, tables, level + 1, merged);
            }
        } catch (RuntimeException e) {
            // Log error and continue to ensure lock is released
            System.err.println("Failed to run compaction: " + e.getMessage());
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
