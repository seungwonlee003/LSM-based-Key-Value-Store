public class Memtable implements Iterable<Map.Entry<String, String>> {

    private final NavigableMap<String, String> table = new TreeMap<>();
    private long sizeBytes = 0L;

    public Memtable(){
    }

    public String get(String key) {
        return table.get(key);
    }

    public void put(String key, String value) {
        Objects.requireNonNull(key, "key");
        String old = table.put(key, value);
        if (old != null) {
            sizeBytes -= estimateSize(key, old);
        }
        sizeBytes += estimateSize(key, value);
    }

    public long size() {
        return sizeBytes;
    }

    public Iterator<Map.Entry<String, String>> iterator() {
        return table.entrySet().iterator();
    }

    private long estimateSize(String key, String value) {
        int keyLen = key.getBytes(StandardCharsets.UTF_8).length;
        int valLen = (value == null) ? 0 : value.getBytes(StandardCharsets.UTF_8).length;
        return keyLen + valLen;
    }
}
