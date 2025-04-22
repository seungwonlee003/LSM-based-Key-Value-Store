class BloomFilter {
    private final BitSet bitSet;
    private final int size;
    private final int hashCount;

    public BloomFilter(int size, int hashCount) {
        this.size = size;
        this.hashCount = hashCount;
        this.bitSet = new BitSet(size);
    }

    public void add(String key) {
        for (int i = 0; i < hashCount; i++) {
            int hash = hash(key, i);
            bitSet.set(Math.abs(hash % size));
        }
    }

    public boolean mightContain(String key) {
        for (int i = 0; i < hashCount; i++) {
            int hash = hash(key, i);
            if (!bitSet.get(Math.abs(hash % size))) {
                return false;
            }
        }
        return true;
    }

    // Simple hash function (for demonstration)
    private int hash(String key, int seed) {
        int hash = key.hashCode();
        return hash ^ seed;
    }
}
