/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import static org.junit.Assert.assertTrue;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.test.ClientBase;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * Load benchmark for the read audit log. Not part of the regular suite (the class name does not
 * match the surefire pattern); run it with:
 *
 * <pre>
 * mvn -pl zookeeper-server test -Dtest=ReadAuditLoggerBenchmark -Dsurefire-forkcount=1 \
 *     -Dbench.watchers=300 -Dbench.writes=200 -Dbench.readConns=32 -Dbench.readThreads=2 \
 *     -Dbench.seconds=5 -Dbench.rounds=3 -Dbench.out=/path/to/results.txt
 * </pre>
 *
 * Modes: audit off, one line per event, and a one-second aggregation window. Both audit modes
 * write through the logger's built-in queue to a plain file appender, which is the recommended
 * setup. Three scenarios, each run under every mode:
 * <ol>
 *   <li>fanOut: N sessions hold a persistent watch on one znode; one writer updates it W times.
 *   Notifications are not audited, so this should match the audit-off mode.</li>
 *   <li>reads: C connections x T threads issue getData as fast as they can for S seconds.</li>
 *   <li>mixed: reads and the fan-out writer at the same time, on the shared request pipeline.</li>
 * </ol>
 * Each result line reports the audit lines written and the lines dropped because the queue was
 * full, taken from the {@code event=dropped} lines.
 */
public class ReadAuditLoggerBenchmark extends ClientBase {

    private static final int WATCHERS = Integer.getInteger("bench.watchers", 300);
    private static final int WRITES = Integer.getInteger("bench.writes", 200);
    private static final int READ_CONNS = Integer.getInteger("bench.readConns", 32);
    private static final int READ_THREADS = Integer.getInteger("bench.readThreads", 2);
    private static final int SECONDS = Integer.getInteger("bench.seconds", 5);
    private static final int ROUNDS = Integer.getInteger("bench.rounds", 2);
    private static final String OUT = System.getProperty("bench.out");

    private static final String HOT = "/bench/hot";
    private static final String READ = "/bench/read";
    private static final byte[] PAYLOAD = new byte[256];

    enum Mode { OFF, PER_EVENT, AGGREGATE_1S }

    private ch.qos.logback.classic.Logger auditLogger;
    private File auditFile;
    private final StringBuilder report = new StringBuilder();

    // ---------------------------------------------------------------- mode setup

    private void apply(Mode mode) throws IOException {
        auditLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ReadAuditLogger.LOGGER_NAME);
        auditLogger.setAdditive(false);
        auditLogger.detachAndStopAllAppenders();
        auditFile = File.createTempFile("read-audit-" + mode + "-", ".log");
        auditFile.deleteOnExit();
        if (mode == Mode.OFF) {
            ReadAuditLogger.configure(false, "", 0, ReadAuditLogger.AggregateBy.SESSION, 10000);
            return;
        }
        LoggerContext ctx = auditLogger.getLoggerContext();
        PatternLayoutEncoder enc = new PatternLayoutEncoder();
        enc.setContext(ctx);
        enc.setPattern("%d{ISO8601} %m%n");
        enc.start();
        FileAppender<ILoggingEvent> file = new FileAppender<>();
        file.setContext(ctx);
        file.setName("READAUDIT");
        file.setFile(auditFile.getPath());
        file.setAppend(false);
        file.setEncoder(enc);
        file.start();
        auditLogger.addAppender(file);
        long windowMs = mode == Mode.AGGREGATE_1S ? 1000 : 0;
        ReadAuditLogger.configure(true, "", windowMs, ReadAuditLogger.AggregateBy.SESSION, 10000);
    }

    /** Disables the audit (flushing any open window), waits for the writer, and counts lines and drops. */
    private String finish() throws Exception {
        ReadAuditLogger.configure(false, "", 0, ReadAuditLogger.AggregateBy.SESSION, 10000);
        assertTrue("audit writer did not drain", ReadAuditLogger.awaitDrained(120_000));
        auditLogger.detachAndStopAllAppenders();
        long lines = 0;
        long dropped = 0;
        if (auditFile.exists()) {
            try (Stream<String> s = Files.lines(auditFile.toPath())) {
                for (String l : (Iterable<String>) s::iterator) {
                    int i = l.indexOf("event=dropped count=");
                    if (i >= 0) {
                        String rest = l.substring(i + "event=dropped count=".length());
                        dropped += Long.parseLong(rest.substring(0, rest.indexOf(' ')));
                    } else {
                        lines++;
                    }
                }
            }
        }
        auditFile.delete();
        return "lines=" + lines + " dropped=" + dropped;
    }

    // ---------------------------------------------------------------- stats

    private static final class Lat {
        long[] v = new long[4096];
        int n;

        synchronized void add(long nanos) {
            if (n == v.length) {
                v = Arrays.copyOf(v, n * 2);
            }
            v[n++] = nanos;
        }

        synchronized void addAll(Lat o) {
            for (int i = 0; i < o.n; i++) {
                add(o.v[i]);
            }
        }

        synchronized String summary() {
            if (n == 0) {
                return "n=0";
            }
            long[] s = Arrays.copyOf(v, n);
            Arrays.sort(s);
            return String.format(Locale.ENGLISH, "n=%d p50=%.0fus p99=%.0fus max=%.0fus",
                    n, s[n / 2] / 1e3, s[(int) Math.min(n - 1, Math.round(n * 0.99))] / 1e3, s[n - 1] / 1e3);
        }
    }

    private void line(String s) {
        System.out.println("BENCH " + s);
        report.append(s).append('\n');
    }

    // ---------------------------------------------------------------- scenarios

    private final LongAdder delivered = new LongAdder();

    private String fanOut(ZooKeeper writer, int writes, boolean measure) throws Exception {
        long expected = delivered.sum() + (long) writes * WATCHERS;
        Lat lat = new Lat();
        long t0 = System.nanoTime();
        for (int i = 0; i < writes; i++) {
            long s = System.nanoTime();
            writer.setData(HOT, PAYLOAD, -1);
            lat.add(System.nanoTime() - s);
        }
        long writeEnd = System.nanoTime();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (delivered.sum() < expected && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        long t1 = System.nanoTime();
        long got = delivered.sum();
        assertTrue("notifications lost: " + got + " < " + expected, got >= expected);
        if (!measure) {
            return null;
        }
        double writeSec = (writeEnd - t0) / 1e9;
        double allSec = (t1 - t0) / 1e9;
        return String.format(Locale.ENGLISH,
                "writes/s=%.0f write-latency[%s] delivery-wall=%.0fms notifications/s=%.0f",
                writes / writeSec, lat.summary(), allSec * 1e3, (long) writes * WATCHERS / allSec);
    }

    private String reads(List<ZooKeeper> conns, int seconds, boolean measure, ZooKeeper concurrentWriter)
            throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(conns.size() * READ_THREADS + 1);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        List<Future<Lat>> readers = new ArrayList<>();
        for (ZooKeeper zk : conns) {
            for (int t = 0; t < READ_THREADS; t++) {
                readers.add(pool.submit(() -> {
                    Lat l = new Lat();
                    while (System.nanoTime() < deadline) {
                        long s = System.nanoTime();
                        zk.getData(READ, false, null);
                        l.add(System.nanoTime() - s);
                    }
                    return l;
                }));
            }
        }
        Future<Lat> writerLat = null;
        long deliveredBefore = delivered.sum();
        if (concurrentWriter != null) {
            writerLat = pool.submit(() -> {
                Lat l = new Lat();
                while (System.nanoTime() < deadline) {
                    long s = System.nanoTime();
                    concurrentWriter.setData(HOT, PAYLOAD, -1);
                    l.add(System.nanoTime() - s);
                }
                return l;
            });
        }
        Lat all = new Lat();
        for (Future<Lat> f : readers) {
            all.addAll(f.get());
        }
        Lat w = writerLat == null ? null : writerLat.get();
        pool.shutdown();
        if (w != null) {
            // let the last notifications drain before the next mode so counts stay comparable
            long expected = deliveredBefore + (long) w.n * WATCHERS;
            long drain = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
            while (delivered.sum() < expected && System.nanoTime() < drain) {
                Thread.sleep(1);
            }
        }
        if (!measure) {
            return null;
        }
        String out = String.format(Locale.ENGLISH, "reads/s=%.0f read-latency[%s]", all.n / (double) seconds, all.summary());
        if (w != null) {
            out += String.format(Locale.ENGLISH, " writes/s=%.0f write-latency[%s] notifications=%d",
                    w.n / (double) seconds, w.summary(), delivered.sum() - deliveredBefore);
        }
        return out;
    }

    // ---------------------------------------------------------------- driver

    @Test
    public void benchmark() throws Exception {
        ZooKeeper admin = createClient();
        admin.create("/bench", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        admin.create(HOT, PAYLOAD, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        admin.create(READ, PAYLOAD, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        List<ZooKeeper> watchers = new ArrayList<>();
        List<ZooKeeper> readers = new ArrayList<>();
        ZooKeeper writer = createClient();
        try {
            Watcher count = e -> {
                if (e.getType() == Watcher.Event.EventType.NodeDataChanged) {
                    delivered.increment();
                }
            };
            for (int i = 0; i < WATCHERS; i++) {
                ZooKeeper zk = createClient();
                zk.addWatch(HOT, count, AddWatchMode.PERSISTENT);
                watchers.add(zk);
            }
            for (int i = 0; i < READ_CONNS; i++) {
                readers.add(createClient());
            }
            line(String.format(Locale.ENGLISH,
                    "config watchers=%d writes=%d readConns=%d readThreads=%d seconds=%d cpus=%d",
                    WATCHERS, WRITES, READ_CONNS, READ_THREADS, SECONDS, Runtime.getRuntime().availableProcessors()));

            // global warmup with the audit off so JIT and client sessions settle before any mode is measured
            apply(Mode.OFF);
            fanOut(writer, WRITES, false);
            reads(readers, SECONDS * 2, false, null);
            reads(readers, SECONDS, false, writer);
            finish();

            for (int round = 1; round <= ROUNDS; round++) {
                for (Mode mode : Mode.values()) {
                    apply(mode);
                    fanOut(writer, Math.max(1, WRITES / 4), false);
                    String fan = fanOut(writer, WRITES, true);
                    String fanLines = finish();

                    apply(mode);
                    reads(readers, 2, false, null);
                    String rd = reads(readers, SECONDS, true, null);
                    String readLines = finish();

                    apply(mode);
                    reads(readers, 2, false, writer);
                    String mixed = reads(readers, SECONDS, true, writer);
                    String mixedLines = finish();

                    line(String.format(Locale.ENGLISH, "r%d %-12s fanOut  %s %s", round, mode, fan, fanLines));
                    line(String.format(Locale.ENGLISH, "r%d %-12s reads   %s %s", round, mode, rd, readLines));
                    line(String.format(Locale.ENGLISH, "r%d %-12s mixed   %s %s", round, mode, mixed, mixedLines));
                }
            }
        } finally {
            for (ZooKeeper zk : watchers) {
                zk.close();
            }
            for (ZooKeeper zk : readers) {
                zk.close();
            }
            writer.close();
            admin.close();
            if (OUT != null) {
                try (PrintWriter pw = new PrintWriter(new FileWriter(OUT, true))) {
                    pw.print(report);
                }
            }
        }
    }
}
