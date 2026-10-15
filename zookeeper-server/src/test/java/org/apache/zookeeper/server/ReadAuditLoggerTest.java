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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.test.LoggerTestTool;
import org.junit.After;
import org.junit.Test;

public class ReadAuditLoggerTest extends ClientBase {

    static void enable(String prefixes, long windowMs, ReadAuditLogger.AggregateBy by, int maxKeys) {
        ReadAuditLogger.configure(true, prefixes, windowMs, by, maxKeys);
    }

    @After
    public void resetReadAudit() {
        ReadAuditLogger.configure(false, "", 0, ReadAuditLogger.AggregateBy.SESSION, 10000);
    }

    static String grepLines(String out, String needle) {
        StringBuilder sb = new StringBuilder();
        for (String line : out.split("\n")) {
            if (line.contains(needle)) {
                sb.append(line).append('\n');
            }
        }
        return sb.toString();
    }

    static int countLines(String out, String needle) {
        String g = grepLines(out, needle);
        return g.isEmpty() ? 0 : g.split("\n").length;
    }

    @Test
    public void testPathPrefixUnit() {
        java.util.List<String> p = ReadAuditLogger.parsePrefixes(" /app/ , /cfg,bad, ");
        assertEquals(Arrays.asList("/app", "/cfg"), p);
        assertTrue(ReadAuditLogger.pathMatches(p, "/app"));
        assertTrue(ReadAuditLogger.pathMatches(p, "/app/x/y"));
        assertTrue(ReadAuditLogger.pathMatches(p, "/cfg/z"));
        assertFalse(ReadAuditLogger.pathMatches(p, "/application"));
        assertFalse(ReadAuditLogger.pathMatches(p, "/"));
        assertFalse(ReadAuditLogger.pathMatches(p, null));
        assertTrue(ReadAuditLogger.pathMatches(ReadAuditLogger.parsePrefixes(""), "/anything"));
        assertTrue(ReadAuditLogger.pathMatches(ReadAuditLogger.parsePrefixes("/"), "/anything"));
        assertTrue(ReadAuditLogger.pathIsAncestor(p, "/"));
        assertTrue(ReadAuditLogger.pathIsParent(ReadAuditLogger.parsePrefixes("/app/x"), "/app"));
        assertTrue(ReadAuditLogger.pathIsParent(ReadAuditLogger.parsePrefixes("/app"), "/"));
        assertFalse(ReadAuditLogger.pathIsParent(ReadAuditLogger.parsePrefixes("/app/x"), "/"));
        assertFalse(ReadAuditLogger.pathIsParent(ReadAuditLogger.parsePrefixes("/"), "/"));
        assertFalse(ReadAuditLogger.pathIsAncestor(p, "/app"));
        assertFalse(ReadAuditLogger.pathIsAncestor(p, "/ap"));
    }

    @Test
    public void testOnlyConfiguredSubtreeIsLogged() throws Exception {
        enable("/app", 0, ReadAuditLogger.AggregateBy.SESSION, 10000);
        try (LoggerTestTool tool = new LoggerTestTool(ReadAuditLogger.LOGGER_NAME)) {
            ZooKeeper zk = createClient();
            try {
                for (String n : new String[]{"/app", "/app/x", "/app/x/y", "/application", "/other"}) {
                    zk.create(n, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                zk.getData("/app", false, null);
                zk.getData("/app/x/y", true, null);
                zk.getData("/application", true, null);
                zk.getData("/other", true, null);
                zk.getChildren("/", false);
                zk.multi(Arrays.asList(Op.getData("/app/x"), Op.getData("/other"), Op.getData("/app/missing")));
                zk.addWatch("/", AddWatchMode.PERSISTENT_RECURSIVE);
                zk.addWatch("/other", AddWatchMode.PERSISTENT);

                zk.setData("/other", new byte[1], -1);
                zk.setData("/app/x/y", new byte[1], -1);
                String out = waitFor(tool, "op=addWatch path=/ watchMode=PERSISTENT_RECURSIVE");
                // give the two writes time to fire the watches; notifications must not be logged
                Thread.sleep(300);
                out = tool.getOutputStream().toString();
                System.out.println("==== READ AUDIT LOG (prefix /app) ====\n" + out + "==== END ====");

                assertTrue(out.contains("op=getData path=/app watch=false result=OK"));
                assertTrue(out.contains("op=getData path=/app/x/y watch=true result=OK"));
                assertTrue(out.contains("op=multiRead.getData path=/app/x result=OK"));
                assertTrue(out.contains("op=multiRead.getData path=/app/missing result=NONODE"));
                assertTrue("recursive watch on an ancestor covers /app",
                        out.contains("op=addWatch path=/ watchMode=PERSISTENT_RECURSIVE result=OK"));
                assertFalse(out.contains("event=watchFired"));
                assertFalse(out.contains("/application"));
                assertFalse(out.contains("/other"));
                assertFalse(out.contains("op=getChildren"));
            } finally {
                zk.close();
            }
        }
    }

    @Test
    public void testSetWatchesIsFilteredPerPath() throws Exception {
        enable("/app", 0, ReadAuditLogger.AggregateBy.SESSION, 10000);
        try (LoggerTestTool tool = new LoggerTestTool(ReadAuditLogger.LOGGER_NAME)) {
            ZooKeeper zk = createClient();
            try {
                zk.create("/app", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                zk.create("/other", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                zk.getData("/app", true, null);
                zk.getData("/other", true, null);
                stopServer();
                startServer();
                String out = waitFor(tool, "op=setWatches path=/app watchType=data");
                Thread.sleep(200);
                assertFalse(tool.getOutputStream().toString().contains("/other"));
            } finally {
                zk.close();
            }
        }
    }

    @Test
    public void testRepeatedReadsAreAggregatedPerSession() throws Exception {
        // long window so only the explicit flush() emits; keeps the test deterministic
        enable("/app", 3_600_000, ReadAuditLogger.AggregateBy.SESSION, 10000);
        try (LoggerTestTool tool = new LoggerTestTool(ReadAuditLogger.LOGGER_NAME)) {
            ZooKeeper zk1 = createClient();
            ZooKeeper zk2 = createClient();
            try {
                zk1.create("/app", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                for (int i = 0; i < 50; i++) {
                    zk1.getData("/app", false, null);
                }
                for (int i = 0; i < 3; i++) {
                    zk1.exists("/app", false);
                    zk2.getData("/app", false, null);
                }
                zk1.getData("/app", true, null);
                zk2.setData("/app", new byte[1], -1);
                Thread.sleep(300);
                assertEquals("nothing is logged before the window closes", "", tool.getOutputStream().toString());

                ReadAuditLogger.flush();
                String out = tool.getOutputStream().toString();
                System.out.println("==== READ AUDIT LOG (aggregate by session) ====\n" + out + "==== END ====");
                String s1 = "session=0x" + Long.toHexString(zk1.getSessionId());
                String s2 = "session=0x" + Long.toHexString(zk2.getSessionId());

                String getData1 = grepLines(grepLines(out, s1), "op=getData path=/app watch=false");
                assertTrue(getData1, getData1.contains("count=50 first="));
                assertTrue(grepLines(grepLines(out, s1), "op=exists").contains("count=3 "));
                assertTrue(grepLines(grepLines(out, s1), "watch=true").contains("count=1 "));
                assertTrue(grepLines(grepLines(out, s2), "op=getData path=/app watch=false").contains("count=3 "));
                assertFalse(out.contains("watchFired"));
                assertEquals(4, countLines(out, "event=aggregate"));
            } finally {
                zk1.close();
                zk2.close();
            }
        }
    }

    @Test
    public void testAggregateByUserMergesSessions() throws Exception {
        enable("/app", 3_600_000, ReadAuditLogger.AggregateBy.USER, 10000);
        try (LoggerTestTool tool = new LoggerTestTool(ReadAuditLogger.LOGGER_NAME)) {
            ZooKeeper zk1 = createClient();
            ZooKeeper zk2 = createClient();
            try {
                zk1.create("/app", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                for (int i = 0; i < 4; i++) {
                    zk1.getData("/app", false, null);
                    zk2.getData("/app", false, null);
                }
                ReadAuditLogger.flush();
                String out = tool.getOutputStream().toString();
                System.out.println("==== READ AUDIT LOG (aggregate by user) ====\n" + out + "==== END ====");
                String line = grepLines(out, "op=getData path=/app");
                assertEquals(1, countLines(out, "op=getData path=/app"));
                assertTrue(line, line.contains("user=127.0.0.1 clientIp=127.0.0.1 op=getData"));
                assertTrue(line, line.contains("count=8 "));
                assertFalse(line.contains("session="));
            } finally {
                zk1.close();
                zk2.close();
            }
        }
    }

    @Test
    public void testOverflowLogsIndividuallyInsteadOfDropping() throws Exception {
        enable("", 3_600_000, ReadAuditLogger.AggregateBy.SESSION, 2);
        try (LoggerTestTool tool = new LoggerTestTool(ReadAuditLogger.LOGGER_NAME)) {
            ZooKeeper zk = createClient();
            try {
                for (int i = 0; i < 5; i++) {
                    zk.exists("/n" + i, false);
                }
                String before = tool.getOutputStream().toString();
                assertEquals(3, countLines(before, "event=request"));
                ReadAuditLogger.flush();
                String out = tool.getOutputStream().toString();
                assertEquals(2, countLines(out, "event=aggregate "));
                assertTrue(out.contains("event=aggregateOverflow"));
                assertTrue(out.contains("individuallyLogged=3"));
            } finally {
                zk.close();
            }
        }
    }

    /** Sums the reads a log represents: aggregate lines carry count=N, individual lines count once. */
    static long sumReads(String out, String needle) {
        long total = 0;
        for (String line : out.split("\n")) {
            if (!line.contains(needle)) {
                continue;
            }
            int i = line.indexOf(" count=");
            if (line.contains("event=aggregate ") && i >= 0) {
                int end = line.indexOf(' ', i + 7);
                total += Long.parseLong(line.substring(i + 7, end < 0 ? line.length() : end));
            } else if (line.contains("event=request ")) {
                total += 1;
            }
        }
        return total;
    }

    @Test
    public void testConcurrentReadsAreCountedExactly() throws Exception {
        // tiny window and tiny key bound: the flusher swaps windows and overflow kicks in while reads race
        enable("/app", 20, ReadAuditLogger.AggregateBy.SESSION, 3);
        final int clients = 8;
        final int readsPerClient = 500;
        try (LoggerTestTool tool = new LoggerTestTool(ReadAuditLogger.LOGGER_NAME)) {
            ZooKeeper setup = createClient();
            setup.create("/app", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            setup.create("/app/c", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            setup.close();

            java.util.List<ZooKeeper> zks = new java.util.ArrayList<>();
            java.util.List<Thread> threads = new java.util.ArrayList<>();
            java.util.concurrent.atomic.AtomicReference<Throwable> failure = new java.util.concurrent.atomic.AtomicReference<>();
            CountDownLatch start = new CountDownLatch(1);
            for (int i = 0; i < clients; i++) {
                ZooKeeper zk = createClient();
                zks.add(zk);
                Thread t = new Thread(() -> {
                    try {
                        start.await();
                        for (int r = 0; r < readsPerClient; r++) {
                            zk.getData("/app/c", false, null);
                        }
                    } catch (Throwable e) {
                        failure.set(e);
                    }
                });
                threads.add(t);
                t.start();
            }
            start.countDown();
            for (Thread t : threads) {
                t.join(60_000);
            }
            assertEquals(null, failure.get());
            // stops the flusher, waits for it, then flushes the last window
            ReadAuditLogger.configure(false, "", 0, ReadAuditLogger.AggregateBy.SESSION, 10000);
            for (ZooKeeper zk : zks) {
                zk.close();
            }
            String out = tool.getOutputStream().toString();
            long total = sumReads(out, "op=getData path=/app/c ");
            System.out.println("==== concurrency: aggregate lines=" + countLines(out, "event=aggregate ")
                    + " individual lines=" + countLines(out, "event=request ")
                    + " overflow lines=" + countLines(out, "event=aggregateOverflow") + " total reads=" + total);
            assertEquals((long) clients * readsPerClient, total);
            assertTrue("overflow path should have been exercised", out.contains("event=aggregateOverflow"));
            assertTrue("more than one window should have been flushed", countLines(out, "event=aggregate ") > clients);
        }
    }

    @Test
    public void testAuthenticatedUserAndNoAuthAreLogged() throws Exception {
        enable("/app", 0, ReadAuditLogger.AggregateBy.SESSION, 10000);
        try (LoggerTestTool tool = new LoggerTestTool(ReadAuditLogger.LOGGER_NAME)) {
            ZooKeeper alice = createClient();
            ZooKeeper anon = createClient();
            try {
                alice.addAuthInfo("digest", "alice:secret".getBytes());
                alice.create("/app", new byte[0], Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
                alice.getData("/app", false, null);
                try {
                    anon.getData("/app", false, null);
                    fail("expected NoAuth");
                } catch (KeeperException.NoAuthException expected) {
                }
                String out = waitFor(tool, "result=NOAUTH");
                System.out.println("==== READ AUDIT LOG (auth) ====\n" + out + "==== END ====");
                String aliceLine = grepLines(out, "session=0x" + Long.toHexString(alice.getSessionId()));
                assertTrue(aliceLine, aliceLine.contains("user=alice,127.0.0.1 op=getData path=/app watch=false result=OK"));
                String anonLine = grepLines(out, "session=0x" + Long.toHexString(anon.getSessionId()));
                assertTrue(anonLine, anonLine.contains("user=127.0.0.1 op=getData path=/app watch=false result=NOAUTH"));
            } finally {
                alice.close();
                anon.close();
            }
        }
    }

    @Test
    public void testChildWatchOnDirectParentIsInScope() throws Exception {
        enable("/app/x", 0, ReadAuditLogger.AggregateBy.SESSION, 10000);
        try (LoggerTestTool tool = new LoggerTestTool(ReadAuditLogger.LOGGER_NAME)) {
            ZooKeeper reader = createClient();
            ZooKeeper writer = createClient();
            try {
                writer.create("/app", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                CountDownLatch appChildren = new CountDownLatch(1);
                CountDownLatch rootChildren = new CountDownLatch(1);
                reader.getChildren("/app", e -> appChildren.countDown());
                reader.getChildren("/", e -> rootChildren.countDown());
                reader.getChildren("/app", false);
                reader.addWatch("/app", AddWatchMode.PERSISTENT);
                reader.addWatch("/", AddWatchMode.PERSISTENT);

                writer.create("/app/x", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                writer.create("/zzz", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                assertTrue(appChildren.await(10, TimeUnit.SECONDS));
                assertTrue(rootChildren.await(10, TimeUnit.SECONDS));
                Thread.sleep(300);
                String out = tool.getOutputStream().toString();
                System.out.println("==== READ AUDIT LOG (prefix /app/x, parent scope) ====\n" + out + "==== END ====");

                // a child watch on /app reports creation and deletion of /app/x
                assertEquals(1, countLines(out, "op=getChildren path=/app watch=true result=OK"));
                assertEquals(1, countLines(out, "op=addWatch path=/app watchMode=PERSISTENT result=OK"));
                // the notifications those watches received are not logged
                assertFalse(out.contains("watchFired"));
                // a plain listing of /app without a watch is not about /app/x
                assertEquals(0, countLines(out, "op=getChildren path=/app watch=false"));
                // the root is not the direct parent of /app/x
                assertFalse(out.contains("path=/ "));
            } finally {
                reader.close();
                writer.close();
            }
        }
    }

    @Test
    public void testStatusLineReportsWhetherAuditIsActive() throws Exception {
        ch.qos.logback.classic.Logger auditLogger =
                (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ReadAuditLogger.LOGGER_NAME);
        ch.qos.logback.classic.Level original = auditLogger.getLevel();
        try (LoggerTestTool tool = new LoggerTestTool(ReadAuditLogger.class)) {
            enable("/app", 0, ReadAuditLogger.AggregateBy.SESSION, 10000);
            String out = tool.getOutputStream().toString();
            assertTrue(out, out.contains("Read audit log active: logger=" + ReadAuditLogger.LOGGER_NAME + " pathPrefixes=[/app]"));

            auditLogger.setLevel(ch.qos.logback.classic.Level.WARN);
            enable("/app", 0, ReadAuditLogger.AggregateBy.SESSION, 10000);
            out = tool.getOutputStream().toString();
            assertTrue(out, out.contains("is not at INFO, so nothing will be logged"));
            assertFalse(ReadAuditLogger.isEnabled());
        } finally {
            auditLogger.setLevel(original);
        }
    }

    @Test
    public void testDetailsAreConstants() {
        assertTrue(ReadAuditLogger.watchDetail(true) == ReadAuditLogger.watchDetail(true));
        assertEquals("watch=false", ReadAuditLogger.watchDetail(false));
        assertTrue(ReadAuditLogger.watcherTypeDetail(Watcher.WatcherType.Data)
                == ReadAuditLogger.watcherTypeDetail(Watcher.WatcherType.Data));
        assertEquals("watcherType=Children", ReadAuditLogger.watcherTypeDetail(Watcher.WatcherType.Children));
        assertEquals("watchMode=PERSISTENT_RECURSIVE",
                ReadAuditLogger.watchModeDetail(org.apache.zookeeper.ZooDefs.AddWatchModes.persistentRecursive));
    }

    @Test
    public void testWatchNotificationsAreNotLogged() throws Exception {
        enable("/app", 0, ReadAuditLogger.AggregateBy.SESSION, 10000);
        try (LoggerTestTool tool = new LoggerTestTool(ReadAuditLogger.LOGGER_NAME)) {
            ZooKeeper writer = createClient();
            writer.create("/app", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            java.util.List<ZooKeeper> readers = new java.util.ArrayList<>();
            CountDownLatch delivered = new CountDownLatch(3 * 2);
            try {
                for (int i = 0; i < 3; i++) {
                    ZooKeeper zk = createClient();
                    zk.addWatch("/app", e -> delivered.countDown(), AddWatchMode.PERSISTENT);
                    readers.add(zk);
                }
                writer.setData("/app", new byte[1], -1);
                writer.setData("/app", new byte[2], -1);
                // every reader got both notifications
                assertTrue(delivered.await(10, TimeUnit.SECONDS));
                Thread.sleep(300);
                String out = tool.getOutputStream().toString();
                // the readers are identified by their registrations, and the deliveries add nothing
                assertEquals(3, countLines(out, "op=addWatch path=/app watchMode=PERSISTENT result=OK"));
                for (ZooKeeper r : readers) {
                    assertTrue(out, out.contains("session=0x" + Long.toHexString(r.getSessionId()) + " "));
                }
                assertFalse(out, out.contains("watchFired"));
            } finally {
                for (ZooKeeper r : readers) {
                    r.close();
                }
                writer.close();
            }
        }
    }

    @Test
    public void testFlusherThreadEmitsOnItsOwn() throws Exception {
        enable("/app", 500, ReadAuditLogger.AggregateBy.SESSION, 10000);
        try (LoggerTestTool tool = new LoggerTestTool(ReadAuditLogger.LOGGER_NAME)) {
            ZooKeeper zk = createClient();
            try {
                zk.create("/app", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                for (int i = 0; i < 10; i++) {
                    zk.getData("/app", false, null);
                }
                String out = waitFor(tool, "count=10 ");
                assertTrue(out.contains("event=aggregate source=request"));
            } finally {
                zk.close();
            }
        }
    }

    static String waitFor(LoggerTestTool tool, String needle) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        String out = "";
        while (System.currentTimeMillis() < deadline) {
            out = tool.getOutputStream().toString();
            if (out.contains(needle)) {
                return out;
            }
            Thread.sleep(50);
        }
        fail("Did not find '" + needle + "' in read audit log:\n" + out);
        return out;
    }

    @Test
    public void testReadsAndWatchesAreLogged() throws Exception {
        enable("", 0, ReadAuditLogger.AggregateBy.SESSION, 10000);
        try (LoggerTestTool tool = new LoggerTestTool(ReadAuditLogger.LOGGER_NAME)) {
            ZooKeeper zk = createClient();
            try {
                zk.create("/audit", "v1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                zk.create("/audit/child", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                zk.create("/audit/eph", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

                CountDownLatch dataChanged = new CountDownLatch(1);
                Watcher dataWatcher = e -> {
                    if (e.getType() == Watcher.Event.EventType.NodeDataChanged) {
                        dataChanged.countDown();
                    }
                };

                // Reads, with and without watches
                zk.getData("/audit", dataWatcher, null);
                zk.exists("/audit", false);
                zk.exists("/missing", true);
                zk.getChildren("/audit", false);
                zk.getChildren("/audit", false, null);
                zk.getACL("/audit", null);
                zk.getAllChildrenNumber("/audit");
                zk.getEphemerals("/audit");
                zk.multi(Arrays.asList(Op.getData("/audit"), Op.getChildren("/audit")));
                try {
                    zk.getData("/missing", false, null);
                    fail("expected NoNode");
                } catch (KeeperException.NoNodeException expected) {
                }

                // Watch management
                Watcher persistent = e -> { };
                zk.addWatch("/audit", persistent, AddWatchMode.PERSISTENT_RECURSIVE);
                zk.removeWatches("/audit", persistent, Watcher.WatcherType.Any, false);

                // Write that fires the getData watch
                zk.setData("/audit", "v2".getBytes(), -1);
                assertTrue(dataChanged.await(10, TimeUnit.SECONDS));

                // removeAllWatches is sent to the server as removeWatches
                zk.getData("/audit", true, null);
                zk.removeAllWatches("/audit", Watcher.WatcherType.Data, false);

                String out = waitFor(tool, "op=removeWatches");
                System.out.println("==== READ AUDIT LOG (enabled) ====\n" + out + "==== END ====");

                assertTrue(out.contains("op=getData path=/audit watch=true result=OK"));
                assertTrue(out.contains("op=exists path=/audit watch=false result=OK"));
                assertTrue(out.contains("op=exists path=/missing watch=true result=NONODE"));
                assertTrue(out.contains("op=getChildren path=/audit watch=false result=OK"));
                assertTrue(out.contains("op=getChildren2 path=/audit watch=false result=OK"));
                assertTrue(out.contains("op=getACL path=/audit"));
                assertTrue(out.contains("op=getAllChildrenNumber path=/audit"));
                assertTrue(out.contains("op=getEphemerals path=/audit"));
                assertTrue(out.contains("op=multiRead.getData path=/audit result=OK"));
                assertTrue(out.contains("op=multiRead.getChildren path=/audit result=OK"));
                assertTrue(out.contains("op=getData path=/missing watch=false result=NONODE"));
                assertTrue(out.contains("op=addWatch path=/audit watchMode=PERSISTENT_RECURSIVE result=OK"));
                assertTrue(out.contains("op=checkWatches path=/audit watcherType=Any result=OK"));
                assertTrue(out.contains("op=removeWatches path=/audit watcherType=Data result=OK"));
                assertFalse(out, out.contains("event=watchFired"));
                assertTrue(out.contains("user=127.0.0.1"));
                // writes are not part of this log
                assertTrue(!out.contains("op=create") && !out.contains("op=setData"));
            } finally {
                zk.close();
            }
        }
    }

    @Test
    public void testSetWatchesOnReconnectIsLogged() throws Exception {
        enable("", 0, ReadAuditLogger.AggregateBy.SESSION, 10000);
        try (LoggerTestTool tool = new LoggerTestTool(ReadAuditLogger.LOGGER_NAME)) {
            ZooKeeper zk = createClient();
            try {
                zk.create("/rw", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                zk.getData("/rw", true, null);
                zk.getChildren("/rw", true);

                // bounce the server; the client re-registers its watches with setWatches
                stopServer();
                startServer();
                String out = waitFor(tool, "op=setWatches path=/rw watchType=child");
                assertTrue(out.contains("op=setWatches path=/rw watchType=data result=OK"));
                assertTrue(out.contains("op=setWatches path=/rw watchType=child result=OK"));

                // with a persistent watch the client uses setWatches2 instead
                zk.addWatch("/rw", AddWatchMode.PERSISTENT);
                stopServer();
                startServer();
                out = waitFor(tool, "op=setWatches2");
                System.out.println("==== READ AUDIT LOG (reconnect) ====\n" + out + "==== END ====");
                assertTrue(out.contains("op=setWatches2 path=/rw watchType=persistent result=OK"));
            } finally {
                zk.close();
            }
        }
    }

    @Test
    public void testDisabledByDefaultLogsNothing() throws Exception {
        assertTrue(!Boolean.getBoolean(ReadAuditLogger.ENABLED_PROPERTY));
        try (LoggerTestTool tool = new LoggerTestTool(ReadAuditLogger.LOGGER_NAME)) {
            ZooKeeper zk = createClient();
            try {
                zk.create("/off", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                zk.getData("/off", true, null);
                zk.setData("/off", new byte[1], -1);
                Thread.sleep(500);
            } finally {
                zk.close();
            }
            assertEquals("", tool.getOutputStream().toString());
        }
    }
}
