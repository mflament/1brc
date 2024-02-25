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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;

public class CalculateAverage_yah {

    private static final String FILE = "./measurements.txt";
    private static final int MAX_STATION_NAMES = 10000;
    private static final int MAX_STATION_NAME_SIZE = 100;

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
        private final String station;
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        public MeasurementAggregator(String station) {
            this.station = station;
        }

        public void add(double value) {
            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += value;
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

    private static final class Task implements Callable<Collection<MeasurementAggregator>> {
        private final long position;
        private final int size;
        private final byte[] stringBuffer = new byte[MAX_STATION_NAME_SIZE];

        private final Map<String, MeasurementAggregator> aggregators = new HashMap<>(MAX_STATION_NAMES);

        private Task(long position, int size) {
            this.position = position;
            this.size = size;
        }

        @Override
        public Collection<MeasurementAggregator> call() throws IOException {
            MappedByteBuffer buffer;
            try (FileChannel fileChannel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
                buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, size);
            }
            if (position > 0)
                skipToNextLine(buffer);
            long endPosition = position + size;
            while (buffer.position() < endPosition) {
                String station = nextString(buffer, StandardCharsets.UTF_8);
                if (station == null)
                    break;
                String value = nextString(buffer, StandardCharsets.US_ASCII);
                if (value == null)
                    break;
                aggregators.computeIfAbsent(station, MeasurementAggregator::new).add(Double.parseDouble(value));
            }
            return aggregators.values();
        }

        private void skipToNextLine(MappedByteBuffer buffer) {
            while (buffer.hasRemaining()) {
                if (buffer.get() == '\n') break;
            }
        }

        private String nextString(MappedByteBuffer buffer, Charset charset) {
            int length = 0;
            while (buffer.hasRemaining()) {
                byte b = buffer.get();
                if (b == ';' || b == '\n')
                    return length > 0 ? new String(stringBuffer, 0, length, charset) : null;
                stringBuffer[length++] = b;
            }
            return null;
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        long start = System.currentTimeMillis();
        Path path = Paths.get(FILE);
        long fileSize = Files.size(path);
        int threads = Runtime.getRuntime().availableProcessors();
        int chunkSize = (int) Math.min(Math.ceilDiv(fileSize, threads), Integer.MAX_VALUE);
        List<Future<Collection<MeasurementAggregator>>> futures = new ArrayList<>();
        try (ExecutorService executor = Executors.newFixedThreadPool(threads)) {
            long offset = 0;
            while (offset < fileSize) {
                long remaining = fileSize - offset;
                long size = Math.min(remaining, chunkSize);
                futures.add(executor.submit(new Task(offset, (int) size)));
                offset += size;
            }
        }
        Map<String, MeasurementAggregator> aggregators = new HashMap<>(MAX_STATION_NAMES);
        for (Future<Collection<MeasurementAggregator>> future : futures) {
            future.get().forEach(aggregator -> aggregators.merge(aggregator.station, aggregator, MeasurementAggregator::merge));
        }
        Map<String, ResultRow> measurements = new TreeMap<>();
        aggregators.forEach((name, agg) -> measurements.put(name, new ResultRow(agg)));
        long time = System.currentTimeMillis() - start;
        System.out.println(measurements);
        System.err.printf("time=%dms : %02d:%02d:%03d",
                time, Math.floorDiv(time / 1000, 60), Math.floorMod(time / 1000, 60), Math.floorMod(time, 1000));
    }
}
