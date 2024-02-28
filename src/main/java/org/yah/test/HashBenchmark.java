package org.yah.test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class HashBenchmark {

    public static void main(String[] args) throws IOException {
        Names names = Names.load();
        names.forEach((bytes, offset, length) -> {
            int expected = fromHashCode(bytes, offset, length);
            int actual = fromHashCodeUnrolled(bytes, offset, length);
            if (expected != actual)
                System.err.println("Mismatch for " + new String(bytes, offset, length, StandardCharsets.UTF_8));
        });

        int seed = 0;
        Murmur3 murmur3 = new Murmur3(seed);
        XXHash32 xxh32 = new XXHash32(seed);

        System.out.println("murmur3 " + check(murmur3, names));
        System.out.println("xxh32 " + check(xxh32, names));
        System.out.println("fromHashCode " + check(HashBenchmark::fromHashCode, names));
        System.out.println("fromHashCodeUnrolled " + check(HashBenchmark::fromHashCodeUnrolled, names));

        int iter = 100000000;
        System.out.println("murmur3 " + bench(murmur3, names, iter));
        System.out.println("xxh32 " + bench(xxh32, names, iter));
        System.out.println("fromHashCode " + bench(HashBenchmark::fromHashCode, names, iter));
        System.out.println("fromHashCodeUnrolled " + bench(HashBenchmark::fromHashCodeUnrolled, names, iter));

        System.out.println("murmur3 " + bench(murmur3, names, iter));
        System.out.println("xxh32 " + bench(xxh32, names, iter));
        System.out.println("fromHashCode " + bench(HashBenchmark::fromHashCode, names, iter));
        System.out.println("fromHashCodeUnrolled " + bench(HashBenchmark::fromHashCodeUnrolled, names, iter));

    }

    private static BenchResult bench(HashFactory hashFactory, Names names, int iter) {
        Random random = new Random();
        int[] hashes = new int[iter];
        long start = System.currentTimeMillis();
        for (int i = 0; i < iter; i++) {
            int nameIdx = random.nextInt(names.count);
            hashes[i] = hashFactory.hash(names.bytes, nameIdx * 100, names.lengths[nameIdx]);
        }
        long time = System.currentTimeMillis() - start;
        return new BenchResult(time, hashes);
    }

    private record BenchResult(long time, int[] hashes) {
    }

    private static int fromHashCode(byte[] s, int offset, int length) {
        int end = offset + length;
        int h = 0;
        for (int i = offset; i < end; i++) {
            h = 31 * h + s[i];
        }
        return h;
    }

    private static int fromHashCodeUnrolled(byte[] s, int offset, int length) {
        int end = offset + length;
        int i = offset, h = 0;
        for (; i + 3 < end; i += 4) {
            h = 31 * 31 * 31 * 31 * h
                    + 31 * 31 * 31 * s[i]
                    + 31 * 31 * s[i + 1]
                    + 31 * s[i + 2]
                    + s[i + 3];
        }
        for (; i < end; i++) {
            h = 31 * h + s[i];
        }
        return h;
    }

    private static CheckResult check(HashFactory hashFactory, Names names) {
        Map<Integer, Set<String>> hashes = new HashMap<>(names.count());
        names.forEach((bytes, offset, length) -> {
            int hash = hashFactory.hash(bytes, offset, length);
            Set<String> hashNames = hashes.computeIfAbsent(hash, _ -> new HashSet<>());
            hashNames.add(new String(bytes, offset, length, StandardCharsets.UTF_8));
        });

        double avg = hashes.values().stream()
                .mapToInt(Set::size)
                .average().orElseThrow();
        int max = hashes.values().stream()
                .mapToInt(Set::size)
                .max().orElseThrow();
        return new CheckResult(max, avg);
    }

    record CheckResult(int max, double avg) {
    }

    @FunctionalInterface
    interface BytesConsumer {
        void accept(byte[] bytes, int offset, int length);
    }

    record Names(int count, int[] lengths, byte[] bytes) implements Iterable<String> {

    String getString(int index) {
        return new String(bytes, index * 100, lengths[index], StandardCharsets.UTF_8);
    }

    private static Names load() throws IOException {
        List<String> lines = Files.readAllLines(Paths.get("data/weather_stations.csv"));
        List<String> list = lines.stream()
                .filter(l -> !(l.isEmpty() || l.startsWith("#")))
                .map(Names::parseLine)
                .distinct()
                .toList();
        int[] lengths = new int[lines.size()];
        byte[] bytes = new byte[lines.size() * 100];
        int i = 0;
        for (String name : list) {
            byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
            int length = nameBytes.length;
            lengths[i] = length;
            System.arraycopy(nameBytes, 0, bytes, i * 100, length);
            i++;
        }
        return new Names(list.size(), lengths, bytes);
    }

    public void forEach(BytesConsumer consumer) {
        for (int i = 0; i < count; i++) {
            consumer.accept(bytes, i * 100, lengths[i]);
        }
    }

    @Override
    public Iterator<String> iterator() {
        return new Iterator<>() {
            int index = 0;

            @Override
            public boolean hasNext() {
                return index < count;
            }

            @Override
            public String next() {
                if (index >= count)
                    throw new NoSuchElementException();
                return getString(index++);
            }
        };
    }

    private static String parseLine(String line) {
        int i = line.indexOf(';');
        if (i < 0)
            throw new IllegalArgumentException(line);
        return line.substring(0, i);
    }

}

}
