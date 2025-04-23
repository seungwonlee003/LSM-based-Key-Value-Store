private static class SSTableEntry {
    final String key;
    final String value;
    final int sstableNumber;

    SSTableEntry(Map.Entry<String, String> entry, int sstableNumber) {
        this.key = entry.getKey();
        this.value = entry.getValue();
        this.sstableNumber = sstableNumber;
    }
}
