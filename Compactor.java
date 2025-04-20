import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Compactor {

    // Represents a key-value entry with sequence number
    private static class Entry {
        String key;
        String value; // null for tombstone
        long sequenceNumber;

        Entry(String key, String value, long sequenceNumber) {
            this.key = key;
            this.value = value;
            this.sequenceNumber = sequenceNumber;
        }
    }

    // Iterator over SSTable entries
    private static class SSTableIterator implements Iterator<Entry> {
        private final RandomAccessFile file;
        private Entry nextEntry;

        SSTableIterator(SSTable sstable) throws IOException {
            this.file = new RandomAccessFile(sstable.getFilePath(), "r");
            advance();
        }

        private void advance() throws IOException {
            if (file.getFilePointer() >= file.length()) {
                nextEntry = null;
                return;
            }
            int keyLength = file.readInt();
            byte[] keyBytes = new byte[keyLength];
            file.readFully(keyBytes);
            String key = new String(keyBytes, StandardCharsets.UTF_8);
            int valueLength = file.readInt();
            byte[] valueBytes = new byte[valueLength];
            if (valueLength > 0) {
                file.readFully(valueBytes);
            }
            long sequenceNumber = file.readLong();
            nextEntry = new Entry(key, valueLength > 0 ? new String(valueBytes, StandardCharsets.UTF_8) : null, sequenceNumber);
        }

        @Override
        public boolean hasNext() {
            return nextEntry != null;
        }

        @Override
        public Entry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Entry current = nextEntry;
            try {
                advance();
            } catch (IOException e) {
                throw new RuntimeException("Failed to read SSTable", e);
            }
            return current;
        }

        void close() throws IOException {
            file.close();
        }
    }

    // Merges multiple SSTable iterators, keeping the most recent entry for duplicate keys
    private static class MergeIterator implements Iterator<Entry> {
        private final PriorityQueue<IteratorEntry> queue;
        private final Map<String, Long> latestSequenceNumbers;

        private static class IteratorEntry implements Comparable<IteratorEntry> {
            Entry entry;
            SSTableIterator iterator;

            IteratorEntry(Entry entry, SSTableIterator iterator) {
                this.entry = entry;
                this.iterator = iterator;
            }

            @Override
            public int compareTo(IteratorEntry other) {
                int cmp = entry.key.compareTo(other.entry.key);
                return cmp != 0 ? cmp : Long.compare(other.entry.sequenceNumber, entry.sequenceNumber);
            }
        }

        MergeIterator(List<SSTable> inputs) throws IOException {
            queue = new PriorityQueue<>();
            latestSequenceNumbers = new HashMap<>();
            for (SSTable sstable : inputs) {
                SSTableIterator iter = new SSTableIterator(sstable);
                if (iter.hasNext()) {
                    queue.offer(new IteratorEntry(iter.next(), iter));
                }
            }
        }

        @Override
        public boolean hasNext() {
            return !queue.isEmpty();
        }

        @Override
        public Entry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            List<IteratorEntry> entries = new ArrayList<>();
            IteratorEntry top = queue.poll();
            entries.add(top);
            String key = top.entry.key;

            // Collect all entries with the same key
            while (!queue.isEmpty() && queue.peek().entry.key.equals(key)) {
                entries.add(queue.poll());
            }

            // Find the entry with the highest sequence number
            Entry latest = null;
            for (IteratorEntry ie : entries) {
                if (latest == null || ie.entry.sequenceNumber > latest.sequenceNumber) {
                    latest = ie.entry;
                }
                // Advance iterator
                if (ie.iterator.hasNext()) {
                    queue.offer(new IteratorEntry(ie.iterator.next(), ie.iterator));
                }
            }

            latestSequenceNumbers.put(key, latest.sequenceNumber);
            return latest;
        }

        void close() throws IOException {
            for (IteratorEntry ie : queue) {
                ie.iterator.close();
            }
            queue.clear();
        }
    }

    public static List<SSTable> mergeAndSplit(List<SSTable> inputs, long targetSize) {
        List<SSTable> output = new ArrayList<>();
        try (MergeIterator mergeIter = new MergeIterator(inputs)) {
            List<Entry> buffer = new ArrayList<>();
            long currentSize = 0;
            long sequenceNumber = 0;

            // Merge and buffer entries
            while (mergeIter.hasNext()) {
                Entry entry = mergeIter.next();
                // Skip outdated entries
                if (mergeIter.latestSequenceNumbers.get(entry.key) != entry.sequenceNumber) {
                    continue;
                }

                // Estimate size (key_length + key + value_length + value + sequence)
                int keySize = 4 + entry.key.getBytes(StandardCharsets.UTF_8).length;
                int valueSize = 4 + (entry.value != null ? entry.value.getBytes(StandardCharsets.UTF_8).length : 0);
                long entrySize = keySize + valueSize + 8;

                // Check if adding this entry exceeds target size
                if (currentSize + entrySize > targetSize && !buffer.isEmpty()) {
                    output.add(createSSTable(buffer, sequenceNumber++));
                    buffer.clear();
                    currentSize = 0;
                }

                buffer.add(entry);
                currentSize += entrySize;
            }

            // Write remaining entries
            if (!buffer.isEmpty()) {
                output.add(createSSTable(buffer, sequenceNumber++));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to merge and split SSTables", e);
        }
        return output;
    }

    private static SSTable createSSTable(List<Entry> entries, long fileSequence) {
        String filePath = "./data/sstable_" + System.nanoTime() + "_" + fileSequence + ".sst";
        BloomFilter bloomFilter = new BloomFilter(1000, 3);
        NavigableMap<String, Long> index = new TreeMap<>();
        long offset = 0;
        int count = 0;
        int indexInterval = 10;

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            for (Entry entry : entries) {
                bloomFilter.add(entry.key);
                byte[] keyBytes = entry.key.getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = entry.value != null ? entry.value.getBytes(StandardCharsets.UTF_8) : new byte[0];
                file.writeInt(keyBytes.length);
                file.write(keyBytes);
                file.writeInt(valueBytes.length);
                file.write(valueBytes);
                file.writeLong(entry.sequenceNumber);

                if (count % indexInterval == 0) {
                    index.put(entry.key, offset);
                }
                offset += 4 + keyBytes.length + 4 + valueBytes.length + 8;
                count++;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create SSTable: " + filePath, e);
        }

        return new SSTable(filePath, bloomFilter, index);
    }
}
