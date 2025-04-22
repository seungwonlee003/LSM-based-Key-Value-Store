private static class SSTableEntry {
    final String key;
    final String value;
    final int iteratorIndex;

    SSTableEntry(Map.Entry<String, String> entry, int iteratorIndex) {
        this.key = entry.getKey();
        this.value = entry.getValue();
        this.iteratorIndex = iteratorIndex;
    }
}
