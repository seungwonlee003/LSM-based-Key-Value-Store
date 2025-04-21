public class Config {
    private final long segmentSize;
    private final long memtableThresholdBytes;
    private final Map<Integer, Integer> levelThresholds;

    public Config(long segmentSize, long memtableThresholdBytes, Map<Integer, Integer> levelThresholds) {
        this.segmentSize = segmentSize;
        this.memtableThresholdBytes = memtableThresholdBytes;
        this.levelThresholds = levelThresholds;
    }

    public long getSegmentSize() {
        return segmentSize;
    }

    public long getMemtableThresholdBytes() {
        return memtableThresholdBytes;
    }

    public int getLevelThreshold(int level) {
        return levelThresholds.getOrDefault(level, Integer.MAX_VALUE);
    }
}
