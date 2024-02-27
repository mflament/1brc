package org.yah.test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class HashBenchmark {

    public static void main(String[] args) throws IOException {
        List<String> names = loadNames();
        check(new Murmur3HashFactory(0), names);
    }

    private static void check(HashFactory hashFactory, List<String> names) {
        byte[] bytes = "Gaillard".getBytes(StandardCharsets.UTF_8);
        int hash = hashFactory.hash(bytes, 0, bytes.length);
        int expected = (int) 2611759141L;
        if (hash != expected)
            System.err.println("failed: " + hash);
        else
            System.out.println("success: " + hash);

//        Map<Integer, Set<String>> hashes = new HashMap<>(names.size());
//        for (String name : names) {
//            byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
//            int hash = hashFactory.hash(bytes, bytes.length);
//            Set<String> hashNames = hashes.computeIfAbsent(hash, _ -> new HashSet<>());
//            hashNames.add(name);
//        }
//        hashes.entrySet().stream().filter(e -> e.getValue().size() > 1).forEach(System.err::println);
    }

    private static List<String> loadNames() throws IOException {
        List<String> lines = Files.readAllLines(Paths.get("data/weather_stations.csv"));
        return lines.stream()
                .filter(l -> !(l.isEmpty() || l.startsWith("#")))
                .map(HashBenchmark::parseLine)
                .distinct()
                .toList();
    }

    private static String parseLine(String line) {
        int i = line.indexOf(';');
        if (i < 0) throw new IllegalArgumentException(line);
        return line.substring(0, i);
    }


}
