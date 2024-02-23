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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
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

    private static final class Measurement {
        private String station;
        private double value;

        public Measurement() {
        }
    }

    private record ResultRow(double min, double mean, double max) {
        public ResultRow(MeasurementAggregator agg) {
            this(agg.min, (Math.round(agg.sum * 10.0) / 10.0) / agg.count, agg.max);
        }

        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        public MeasurementAggregator(Measurement m) {
            add(m);
        }

        public void add(Measurement m) {
            min = Math.min(min, m.value);
            max = Math.max(max, m.value);
            sum += m.value;
            count++;
        }

        public MeasurementAggregator merge(MeasurementAggregator other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            sum += other.sum;
            count += other.count;
            return this;
        }
    }

    private static final class MeasurementStream {
        private static final int MAX_STATION_NAME_SIZE = 100;
//        private static final int MAX_VALUE_SIZE = 5;
//        private static final int MAX_LINE_SIZE = MAX_STATION_NAME_SIZE + 1 + MAX_VALUE_SIZE + 1;

        private final MappedByteBuffer buffer;
        private byte[] nameBuffer= new byte[MAX_STATION_NAME_SIZE];
        private final Measurement measurement = new Measurement();
        private int remaining;

        public MeasurementStream(long startPosition, int size) throws IOException {
            FileChannel fileChannel = FileChannel.open(Paths.get(FILE));
            fileChannel.
            buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, startPosition, size);
            fileChannel.close();
        }

        public Measurement next() throws IOException {
            String s = readStation();
            if (s == null) return null;
            measurement.station = s;
            measurement.value = readValue();
            return measurement;
        }

        private String readStation() {
            int length = 0;
            while (buffer.hasRemaining()) {
                byte b = buffer.get();
                if (b == ';')
                    break;
                nameBuffer[length++] = b;
            }
            if (length == 0)
                return null;
            return new String(nameBuffer, 0, length, StandardCharsets.UTF_8);
        }

        private double readValue() throws IOException {
            return 0;
        }

    }

    private static final class Task implements Callable<Map<String, MeasurementAggregator>> {
        private final long startPosition;
        private final int chunkSize;

        public Task(long startPosition, int chunkSize) {
            this.startPosition = startPosition;
            this.chunkSize = chunkSize;
        }

        @Override
        public Map<String, MeasurementAggregator> call() throws IOException {
            try (MeasurementStream stream = new MeasurementStream(startPosition, chunkSize)) {
                Map<String, MeasurementAggregator> aggregators = new HashMap<>(MAX_STATION_NAMES);
                while (stream.hasNext()) {
                    Measurement m = stream.next();
                    MeasurementAggregator aggregator = aggregators.get(m.station);
                    if (aggregator == null)
                        aggregators.put(m.station, new MeasurementAggregator(m));
                    else
                        aggregator.add(m);
                }
                return aggregators;
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        long start = System.currentTimeMillis();
        Path path = Paths.get(FILE);
        long fileSize = Files.size(path);
        int threadCount = Runtime.getRuntime().availableProcessors();
        TreeMap<String, ResultRow> measurements;
        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            List<Future<Map<String, MeasurementAggregator>>> futures = new LinkedList<>();
            int chunkSize = (int) Math.ceilDiv(fileSize, threadCount);
            for (int i = 0; i < threadCount; i++) {
                futures.add(executor.submit(new Task((long) i * chunkSize, chunkSize)));
            }
            Map<String, MeasurementAggregator> aggregators = new HashMap<>(MAX_STATION_NAMES);
            while (!futures.isEmpty()) {
                Future<Map<String, MeasurementAggregator>> future = futures.removeFirst();
                if (future.isDone()) {
                    Map<String, MeasurementAggregator> threadAggregators = future.get();
                    threadAggregators.forEach((name, aggregator) -> aggregators.merge(name, aggregator, MeasurementAggregator::merge));
                } else {
                    futures.addLast(future);
                }
            }
            measurements = new TreeMap<>();
            aggregators.forEach((name, agg) -> measurements.put(name, new ResultRow(agg)));
        }


//        Collector<Measurement, MeasurementAggregator, ResultRow> collector = Collector.of(
//                MeasurementAggregator::new,
//                (a, m) -> {
//                    a.min = Math.min(a.min, m.value);
//                    a.max = Math.max(a.max, m.value);
//                    a.sum += m.value;
//                    a.count++;
//                },
//                (agg1, agg2) -> {
//                    var res = new MeasurementAggregator();
//                    res.min = Math.min(agg1.min, agg2.min);
//                    res.max = Math.max(agg1.max, agg2.max);
//                    res.sum = agg1.sum + agg2.sum;
//                    res.count = agg1.count + agg2.count;
//
//                    return res;
//                },
//                agg -> {
//                    return new ResultRow(agg.min, (Math.round(agg.sum * 10.0) / 10.0) / agg.count, agg.max);
//                });
//
//        Map<String, ResultRow> measurements = new TreeMap<>(Files.lines(path)
//                .map(l -> new Measurement(l.split(";")))
//                .collect(groupingBy(m -> m.station(), collector)));

        long time = System.currentTimeMillis() - start;
        System.out.println(measurements);
        System.err.println(STR."time=\{time}");
    }
}
