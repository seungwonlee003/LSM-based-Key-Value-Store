import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manifest {
    private final String filePath;
    private final String current;
    private final Map<Integer, List<SSTable>> levelMap = new HashMap<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public Manifest() throws IOException {
        this.filePath = "./data";
        this.current = filePath + "/CURRENT";

        Files.createDirectories(Paths.get(filePath));

        Path currentPath = Paths.get(current);
        if (Files.exists(currentPath)) {
            String manifestFile = Files.readString(currentPath).trim();
            loadManifest(manifestFile);
        } else {
            String manifestFile = generateManifestFileName(1);
            persistToFile(manifestFile);
            Files.writeString(currentPath, manifestFile);
        }
    }

    private void loadManifest(String manifestFile) throws IOException {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath + "/" + manifestFile))) {
            Map<Integer, List<String>> serializedMap = (Map<Integer, List<String>>) ois.readObject();
            for (Map.Entry<Integer, List<String>> entry : serializedMap.entrySet()) {
                int level = entry.getKey();
                List<SSTable> sstables = new ArrayList<>();
                for (String sstablePath : entry.getValue()) {
                    sstables.add(new SSTable(sstablePath));
                }
                levelMap.put(level, sstables);
            }
        }
    }

    public void persist() throws IOException {
        String newManifestFile = generateNextManifestFileName();
        persistToFile(newManifestFile);
        Files.writeString(Paths.get(current), newManifestFile);
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

    public void addSSTable(int level, SSTable sstable) throws IOException {
        levelMap.computeIfAbsent(level, k -> new ArrayList<>()).add(0, sstable);
        persist();
    }
    
    public List<SSTable> getSSTables(int level) {
        return new ArrayList<>(levelMap.getOrDefault(level, new ArrayList<>()));
    }

    public int maxLevel() {
        return levelMap.isEmpty() ? -1 : levelMap.keySet().stream().max(Integer::compare).orElse(-1);
    }

    public void replace(int levelToClear, List<SSTable> newTables) throws IOException {
        levelMap.remove(levelToClear);
        levelMap.remove(levelToClear + 1);
        levelMap.computeIfAbsent(levelToClear + 1, k -> new ArrayList<>()).addAll(newTables);
        persist();
    }
}
