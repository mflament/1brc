/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CalculateAverage_yah {

    private static final String FILE = "./measurements.txt";
    private static final int MAX_STATION_NAMES = 10000;
    private static final int MAX_STATION_NAME_SIZE = 100;

    private record ResultRow(double min, double mean, double max) {
        public ResultRow(StationAggregator aggregator) {
            this(aggregator.min, (Math.round(aggregator.sum * 10.0) / 10.0) / aggregator.count, aggregator.max);
        }

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static final class StationAggregator {
        final int hash;

        final byte[] nameBuffer;
        final int nameOffset;
        final int nameLength;

        StationAggregator next;

        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        private String stationName;

        private StationAggregator(int hash, byte[] nameBuffer, int nameOffset, int nameLength) {
            this.hash = hash;
            this.nameBuffer = nameBuffer;
            this.nameOffset = nameOffset;
            this.nameLength = nameLength;
        }

        public void add(double value) {
            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += value;
            count++;
        }

        public StationAggregator merge(StationAggregator other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            sum += other.sum;
            count += other.count;
            return this;
        }

        public String getStationName() {
            if (stationName == null)
                stationName = new String(nameBuffer, nameOffset, nameLength, StandardCharsets.UTF_8);
            return stationName;
        }

    }

    private static final class AggregatorsMap {

        private final byte[] namesBuffer = new byte[MAX_STATION_NAMES * MAX_STATION_NAME_SIZE];
        private int namesBufferSize;

        private final StationAggregator[] entries = new StationAggregator[MAX_STATION_NAMES];
        private int size;

        StationAggregator get(byte[] name, int length) {
            int hash = createHash(name, length);
            int aggregatorIndex = (int) (Integer.toUnsignedLong(hash) % entries.length);
            StationAggregator aggregator = entries[aggregatorIndex];

            if (aggregator != null) {
                StationAggregator last = null;
                while (aggregator != null && aggregator.hash != hash) {
                    last = aggregator;
                    aggregator = aggregator.next;
                }
                if (aggregator == null)
                    aggregator = last.next = createAggregator(hash, name, length);
                // else {
                // if (!Arrays.equals(aggregator.nameBuffer, aggregator.nameOffset, aggregator.nameOffset + aggregator.nameLength,
                // name, 0, length)) {
                // System.out.println("collision " + aggregator.getStationName() + " != " + new String(namesBuffer, 0, length, StandardCharsets.UTF_8));
                // }
                // }
            }
            else {
                aggregator = entries[aggregatorIndex] = createAggregator(hash, name, length);
            }
            return aggregator;
        }

        private StationAggregator createAggregator(int hash, byte[] name, int length) {
            int nameOffset = namesBufferSize;
            System.arraycopy(name, 0, namesBuffer, nameOffset, length);
            namesBufferSize += length;
            size++;
            return new StationAggregator(hash, namesBuffer, nameOffset, length);
        }

        public Collection<StationAggregator> values() {
            ArrayList<StationAggregator> list = new ArrayList<>(size);
            for (StationAggregator value : entries) {
                StationAggregator aggregator = value;
                while (aggregator != null) {
                    list.add(aggregator);
                    aggregator = aggregator.next;
                }
            }
            return list;
        }

    }

    private static int createHash(byte[] s, int length) {
        int i = 0, h = 0;
        for (; i + 3 < length; i += 4) {
            h = 31 * 31 * 31 * 31 * h
                    + 31 * 31 * 31 * s[i]
                    + 31 * 31 * s[i + 1]
                    + 31 * s[i + 2]
                    + s[i + 3];
        }
        for (; i < length; i++) {
            h = 31 * h + s[i];
        }
        return h;
    }

    // value parsing
    private static double parseValue(byte[] s) {
        int i, sign;
        if (s[0] == '-') {
            i = 1;
            sign = -1;
        }
        else {
            i = 0;
            sign = 1;
        }
        double d = parseDigit(s, i++);
        if (s[i] != '.')
            d = d * 10 + parseDigit(s, i++);
        i++; // skip '.'
        d += parseDigit(s, i) * 0.1;
        return d * sign;
    }

    private static int parseDigit(byte[] s, int index) {
        return (char) s[index] - '0';
    }

    private static final class MappedByteBufferTask implements Callable<Collection<StationAggregator>> {
        private final long position;
        private final int size;

        private final AggregatorsMap aggregators = new AggregatorsMap();

        private MappedByteBufferTask(long position, int size) {
            this.position = position;
            this.size = size;
        }

        @Override
        public Collection<StationAggregator> call() throws IOException {
            MappedByteBuffer buffer;
            try (FileChannel fileChannel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
                buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, size);
            }
            if (position > 0)
                skipToNextLine(buffer);

            long endPosition = position + size;
            byte[] nameBuffer = new byte[MAX_STATION_NAME_SIZE];
            while (buffer.position() < endPosition) {
                int length = nextToken(nameBuffer, buffer, ';');
                if (length == 0)
                    break;

                var aggregator = aggregators.get(nameBuffer, length);
                length = nextToken(nameBuffer, buffer, '\n');
                if (length == 0)
                    break;
                aggregator.add(parseValue(nameBuffer));
            }
            return aggregators.values();
        }

        private static void skipToNextLine(MappedByteBuffer buffer) {
            while (buffer.hasRemaining()) {
                if (buffer.get() == '\n')
                    break;
            }
        }

        private static int nextToken(byte[] dst, MappedByteBuffer src, char separator) {
            int length = 0;
            while (src.hasRemaining()) {
                byte b = src.get();
                if (b == separator)
                    return length;
                dst[length++] = b;
            }
            return 0;
        }

    }

    // main
    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        long start = System.currentTimeMillis();
        Path path = Paths.get(FILE);
        long fileSize = Files.size(path);
        int threads = Runtime.getRuntime().availableProcessors();
        int chunkSize = (int) Math.min(Math.ceilDiv(fileSize, threads), Integer.MAX_VALUE);
        List<Future<Collection<StationAggregator>>> futures = new ArrayList<>();
        try (ExecutorService executor = Executors.newFixedThreadPool(threads)) {
            long offset = 0;
            while (offset < fileSize) {
                long remaining = fileSize - offset;
                long size = Math.min(remaining, chunkSize);
                futures.add(executor.submit(new MappedByteBufferTask(offset, (int) size)));
                offset += size;
            }
        }
        Map<String, StationAggregator> aggregators = new HashMap<>(MAX_STATION_NAMES);
        for (Future<Collection<StationAggregator>> future : futures) {
            future.get().forEach(agg -> aggregators.merge(agg.getStationName(), agg, StationAggregator::merge));
        }
        Map<String, ResultRow> measurements = new TreeMap<>();
        aggregators.forEach((name, agg) -> measurements.put(name, new ResultRow(agg)));
        System.out.println(measurements);
        long time = System.currentTimeMillis() - start;
        System.err.printf("time=%dms : %02d:%02d:%03d", time, Math.floorDiv(time / 1000, 60), Math.floorMod(time / 1000, 60), Math.floorMod(time, 1000));
    }
}
