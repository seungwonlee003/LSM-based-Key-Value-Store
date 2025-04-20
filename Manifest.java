import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manifest {
    private final String filePath;
    private final String current;
    private final Map<Integer, List<SSTable>> levelMap;
    private final ReadWriteLock rwLock;

    // consturctor either loads the manifest and current files if exist or create all of them
    public Manifest() {
        this.filePath = "./data";
        this.current = filePath + "/CURRENT";
        this.levelMap = new HashMap<>();
        this.rwLock = new ReentrantReadWriteLock();

        try {
            Files.createDirectories(Paths.get(filePath));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create data directory: " + filePath, e);
        }

        Path currentPath = Paths.get(current);
        if (Files.exists(currentPath)) {
            try {
                String manifestFile = Files.readString(currentPath).trim();
                loadManifest(manifestFile);
            } catch (IOException e) {
                throw new RuntimeException("Failed to read CURRENT or manifest file", e);
            }
        } else {
            String manifestFile = generateManifestFileName(1);
            try {
                persistToFile(manifestFile);
                Files.writeString(currentPath, manifestFile);
            } catch (IOException e) {
                throw new RuntimeException("Failed to initialize manifest or CURRENT file", e);
            }
        }
    }

    // loads the manifest file and all the sstables into memory
    private void loadManifest(String manifestFile) throws IOException {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath + "/" + manifestFile))) {
            Map<Integer, List<String>> serializedMap = (Map<Integer, List<String>>) ois.readObject();
            rwLock.writeLock().lock();
            try {
                for (Map.Entry<Integer, List<String>> entry : serializedMap.entrySet()) {
                    int level = entry.getKey();
                    List<SSTable> sstables = new ArrayList<>();
                    for (String sstablePath : entry.getValue()) {
                        sstables.add(new SSTable(sstablePath));
                    }
                    levelMap.put(level, sstables);
                }
            } finally {
                rwLock.writeLock().unlock();
            }
        } catch (ClassNotFoundException e) {
            throw new IOException("Invalid manifest file format", e);
        }
    }

    // persist the manifest file into new file and reset the current file's pointer to this
    public void persist() {
        rwLock.writeLock().lock();
        try {
            String newManifestFile = generateNextManifestFileName();
            persistToFile(newManifestFile);
            Files.writeString(Paths.get(current), newManifestFile);
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist manifest", e);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void persistToFile(String manifestFile) throws IOException {
        Map<Integer, List<String>> serializedMap = new HashMap<>();
        for (Map.Entry<Integer, List<SSTable>> entry : levelMap.entrySet()) {
            List<String> sstablePaths = new ArrayList<>();
            for (SSTable sstable : entry.getValue()) {
                sstablePaths.add(sstable.getFilePath());
            }
            serializedMap.put(entry.getKey(), sstablePaths);
        }
        try (ObjectOutputStream oos = new ObjectOutputStream(
                new FileOutputStream(filePath + "/" + manifestFile))) {
            oos.writeObject(serializedMap);
        }
    }

    private String generateManifestFileName(int number) {
        return String.format("MANIFEST-%06d", number);
    }

    private String generateNextManifestFileName() throws IOException {
        int maxNumber = 1;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(filePath), "MANIFEST-*")) {
            for (Path path : stream) {
                String fileName = path.getFileName().toString();
                int number = Integer.parseInt(fileName.substring("MANIFEST-".length()));
                maxNumber = Math.max(maxNumber, number);
            }
        }
        return generateManifestFileName(maxNumber + 1);
    }

    public ReadWriteLock getLock() {
        return rwLock;
    }

    public void addSSTable(int level, SSTable sstable) {
        rwLock.writeLock().lock();
        try {
            levelMap.computeIfAbsent(level, k -> new ArrayList<>()).add(sstable);
            persist();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public List<SSTable> getSSTables(int level) {
        rwLock.readLock().lock();
        try {
            return new ArrayList<>(levelMap.getOrDefault(level, new ArrayList<>()));
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public int maxLevel() {
        rwLock.readLock().lock();
        try {
            return levelMap.isEmpty() ? -1 : levelMap.keySet().stream().max(Integer::compare).orElse(-1);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void replace(int levelToClear, List<SSTable> oldTables, int targetLevel, List<SSTable> newTables) {
        rwLock.writeLock().lock();
        try {
            levelMap.remove(levelToClear);
            levelMap.computeIfAbsent(targetLevel, k -> new ArrayList<>()).addAll(newTables);
            persist();
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}

}
