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

import org.apache.zookeeper.ClientCnxnSocketNetty;
import org.apache.zookeeper.client.ZKClientConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** Runs every read audit test against the Netty server and client connection stack. */
public class ReadAuditLoggerNettyTest extends ReadAuditLoggerTest {

    @BeforeClass
    public static void useNetty() {
        System.setProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY, NettyServerCnxnFactory.class.getName());
        System.setProperty(ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET, ClientCnxnSocketNetty.class.getName());
    }

    @AfterClass
    public static void clearNetty() {
        System.clearProperty(ServerCnxnFactory.ZOOKEEPER_SERVER_CNXN_FACTORY);
        System.clearProperty(ZKClientConfig.ZOOKEEPER_CLIENT_CNXN_SOCKET);
    }

    @Override
    @org.junit.Test
    public void testDisabledByDefaultLogsNothing() throws Exception {
        // also confirm the server under test really is Netty, so the Netty watch hook was exercised
        org.junit.Assert.assertTrue(serverFactory instanceof NettyServerCnxnFactory);
        super.testDisabledByDefaultLogsNothing();
    }
}
