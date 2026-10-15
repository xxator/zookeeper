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
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MultiOperationRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.MultiHeader;
import org.apache.zookeeper.server.watch.IWatchManager;
import org.apache.zookeeper.server.watch.WatchManager;
import org.apache.zookeeper.server.watch.WatchManagerOptimized;
import org.apache.zookeeper.test.LoggerTestTool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Drives FinalRequestProcessor directly to reach failure paths a normal client cannot produce. */
public class ReadAuditLoggerProcessorTest {

    private FinalRequestProcessor processor;
    private ServerCnxn cnxn;

    @Before
    public void setUp() throws Exception {
        ZooKeeperServer zks = new ZooKeeperServer();
        ZKDatabase db = mock(ZKDatabase.class);
        when(db.getNode(eq("/app"))).thenReturn(new DataNode());
        when(db.aclForNode(any(DataNode.class))).thenReturn(Collections.singletonList(
                new ACL(ZooDefs.Perms.ALL, new Id("world", "anyone"))));
        when(db.getData(eq("/app"), any(Stat.class), any())).thenReturn(new byte[0]);
        zks.setZKDatabase(db);
        processor = new FinalRequestProcessor(zks);
        cnxn = mock(ServerCnxn.class);
    }

    @After
    public void tearDown() {
        ReadAuditLogger.configure(false, "", 0, ReadAuditLogger.AggregateBy.SESSION, 10000);
    }

    private static ByteBuffer serialize(Record... records) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
        for (Record r : records) {
            r.serialize(boa, "request");
        }
        return ByteBuffer.wrap(baos.toByteArray());
    }

    private Request request(int type, ByteBuffer bb) {
        return new Request(cnxn, 0x42L, 1, type, bb, new ArrayList<Id>());
    }

    private static ServerCnxn watcher(long sessionId) {
        ServerCnxn w = mock(ServerCnxn.class);
        when(w.getSessionId()).thenReturn(sessionId);
        return w;
    }

    private static void triggerAll(IWatchManager manager) {
        manager.addWatch("/app", watcher(0x1));
        manager.addWatch("/app", watcher(0x2));
        manager.triggerWatch("/app", Watcher.Event.EventType.NodeDataChanged);
    }

    @Test
    public void testWatchDeliveryIsNotLogged() throws Exception {
        ReadAuditLogger.configure(true, "", 0, ReadAuditLogger.AggregateBy.SESSION, 10000);
        try (LoggerTestTool tool = new LoggerTestTool(ReadAuditLogger.LOGGER_NAME)) {
            triggerAll(new WatchManager());
            WatchManagerOptimized optimized = new WatchManagerOptimized();
            try {
                triggerAll(optimized);
            } finally {
                optimized.shutdown();
            }
            // events a reconnecting client missed are delivered by setWatches, also without a line
            DataTree tree = new DataTree();
            tree.createNode("/app", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, 0, 1, 5, 1);
            tree.setWatches(1, Collections.singletonList("/app"), Collections.emptyList(), Collections.emptyList(),
                    Collections.emptyList(), Collections.emptyList(), watcher(0x9));
            assertEquals("", tool.getOutputStream().toString());
        }
    }

    @Test
    public void testSessionMovedReadIsLogged() throws Exception {
        ReadAuditLogger.configure(true, "/app", 0, ReadAuditLogger.AggregateBy.SESSION, 10000);
        try (LoggerTestTool tool = new LoggerTestTool(ReadAuditLogger.LOGGER_NAME)) {
            Request r = request(OpCode.getData, serialize(new GetDataRequest("/app", true)));
            // PrepRequestProcessor sets this when the session's owner check fails
            r.setException(new KeeperException.SessionMovedException());
            processor.processRequest(r);
            verify(cnxn).sendCloseSession();
            String out = tool.getOutputStream().toString();
            assertTrue(out, out.contains("session=0x42 client=unknown user= op=getData path=- result=SESSIONMOVED"));
        }
    }

    @Test
    public void testMultiReadWithUnsupportedFirstOpIsLogged() throws Exception {
        ReadAuditLogger.configure(true, "/app", 0, ReadAuditLogger.AggregateBy.SESSION, 10000);
        try (LoggerTestTool tool = new LoggerTestTool(ReadAuditLogger.LOGGER_NAME)) {
            // a multiRead made only of check ops deserializes, then fails on its first op
            MultiOperationRecord record = new MultiOperationRecord(Arrays.asList(Op.check("/app", -1)));
            processor.processRequest(request(OpCode.multiRead, serialize(record)));
            String out = tool.getOutputStream().toString();
            assertTrue(out, out.contains("op=multiRead path=- result=MARSHALLINGERROR"));
        }
    }

    @Test
    public void testMalformedMultiReadIsLoggedWhateverTheFilter() throws Exception {
        ReadAuditLogger.configure(true, "/app", 0, ReadAuditLogger.AggregateBy.SESSION, 10000);
        try (LoggerTestTool tool = new LoggerTestTool(ReadAuditLogger.LOGGER_NAME)) {
            // mixing a read and a write is rejected while deserializing, before any path is known
            ByteBuffer bb = serialize(
                    new MultiHeader(OpCode.getData, false, -1), new GetDataRequest("/app", false),
                    new MultiHeader(OpCode.check, false, -1), new CheckVersionRequest("/app", -1),
                    new MultiHeader(-1, true, -1));
            processor.processRequest(request(OpCode.multiRead, bb));
            String out = tool.getOutputStream().toString();
            assertTrue(out, out.contains("op=multiRead path=- result=MARSHALLINGERROR"));
        }
    }

    @Test
    public void testServedMultiReadKeepsPerOpResults() throws Exception {
        ReadAuditLogger.configure(true, "/app", 0, ReadAuditLogger.AggregateBy.SESSION, 10000);
        try (LoggerTestTool tool = new LoggerTestTool(ReadAuditLogger.LOGGER_NAME)) {
            MultiOperationRecord record = new MultiOperationRecord(Arrays.asList(
                    Op.getData("/app"), Op.getData("/app/missing")));
            processor.processRequest(request(OpCode.multiRead, serialize(record)));
            String out = tool.getOutputStream().toString();
            assertTrue(out, out.contains("op=multiRead.getData path=/app result=OK"));
            assertTrue(out, out.contains("op=multiRead.getData path=/app/missing result=NONODE"));
        }
    }
}
