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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.TestableZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.test.ClientBase.CountdownWatcher;
import org.apache.zookeeper.test.LoggerTestTool;
import org.apache.zookeeper.test.QuorumBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Reads are served by whichever server the client is connected to, so that server must log them. */
public class ReadAuditLoggerQuorumTest extends QuorumBase {

    @Before
    @Override
    public void setUp() throws Exception {
        // 3 voters plus 2 observers
        setUp(true);
    }

    @After
    public void resetReadAudit() {
        ReadAuditLogger.configure(false, "", 0, ReadAuditLogger.AggregateBy.SESSION, 10000);
    }

    private QuorumPeer peerServing(long sessionId) {
        for (QuorumPeer qp : getPeerList()) {
            ZooKeeperServer zs = qp.getActiveServer();
            if (zs == null || zs.getServerCnxnFactory() == null) {
                continue;
            }
            for (ServerCnxn c : zs.getServerCnxnFactory().getConnections()) {
                if (c.getSessionId() == sessionId) {
                    return qp;
                }
            }
        }
        return null;
    }

    @Test
    public void testFollowerLogsReadsItServes() throws Exception {
        checkReadsLoggedOnce(ServerState.FOLLOWING);
    }

    @Test
    public void testObserverLogsReadsItServes() throws Exception {
        checkReadsLoggedOnce(ServerState.OBSERVING);
    }

    private void checkReadsLoggedOnce(ServerState state) throws Exception {
        ReadAuditLogger.configure(true, "/app", 0, ReadAuditLogger.AggregateBy.SESSION, 10000);
        try (LoggerTestTool tool = new LoggerTestTool(ReadAuditLogger.LOGGER_NAME)) {
            TestableZooKeeper reader = createClient(new CountdownWatcher(), state);
            TestableZooKeeper writer = createClient(new CountdownWatcher(), ServerState.LEADING);
            try {
                QuorumPeer served = peerServing(reader.getSessionId());
                assertNotNull(served);
                assertEquals(state, served.getPeerState());

                writer.create("/app", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                reader.sync("/app", (rc, p, ctx) -> { }, null);
                for (int i = 0; i < 20; i++) {
                    reader.getData("/app", false, null);
                }
                CountDownLatch fired = new CountDownLatch(1);
                Watcher w = e -> {
                    if (e.getType() == Watcher.Event.EventType.NodeDataChanged) {
                        fired.countDown();
                    }
                };
                reader.getData("/app", w, null);
                writer.setData("/app", new byte[1], -1);
                assertTrue(fired.await(10, TimeUnit.SECONDS));
                Thread.sleep(300);

                String out = tool.getOutputStream().toString();
                String sess = "session=0x" + Long.toHexString(reader.getSessionId());
                String reads = ReadAuditLoggerTest.grepLines(out, sess);
                System.out.println("==== READ AUDIT LOG (" + state + " sid=" + served.getId() + ") ====\n" + reads + "==== END ====");
                // exactly once per read: the leader must not log reads served by a learner
                assertEquals(20, ReadAuditLoggerTest.countLines(reads, "op=getData path=/app watch=false result=OK"));
                assertEquals(1, ReadAuditLoggerTest.countLines(reads, "op=getData path=/app watch=true result=OK"));
                assertEquals(1, ReadAuditLoggerTest.countLines(
                        ReadAuditLoggerTest.grepLines(out, "event=watchFired"), "readers=0x" + Long.toHexString(reader.getSessionId()) + "/"));
                // the writer's session is on the leader and wrote only, so it never appears
                assertEquals(0, ReadAuditLoggerTest.countLines(out, "session=0x" + Long.toHexString(writer.getSessionId())));
            } finally {
                reader.close();
                writer.close();
            }
        }
    }
}
