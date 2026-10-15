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

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.util.AuthUtil;
import org.apache.zookeeper.server.watch.WatcherMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * POC: logs read requests, watch registrations and watch notifications.
 *
 * <p>System properties, all read once at startup:
 * <ul>
 *   <li>{@code zookeeper.readAuditLog.enabled} (default false): turns the log on.</li>
 *   <li>{@code zookeeper.readAuditLog.pathPrefixes} (default empty = all paths): comma separated
 *   znodes. Only these znodes and their descendants are logged. {@code /app} matches {@code /app}
 *   and {@code /app/x/y} but not {@code /application}.</li>
 *   <li>{@code zookeeper.readAuditLog.aggregateWindowMs} (default 0 = log every event): when
 *   positive, identical events from the same entity are counted and emitted as one
 *   {@code event=aggregate} line per window.</li>
 *   <li>{@code zookeeper.readAuditLog.aggregateBy} (default session): the entity events are
 *   grouped by. {@code session} keys on session id + client address + user, {@code user} keys on
 *   authenticated user + client IP, merging that client's sessions.</li>
 *   <li>{@code zookeeper.readAuditLog.aggregateMaxKeys} (default 10000): bound on distinct groups
 *   held per window. Events past the bound are logged individually so nothing is lost.</li>
 * </ul>
 *
 * <p>Uses the dedicated logger name {@value #LOGGER_NAME} so it can be routed to its own appender.
 * The logger must be at INFO or finer: routing it at WARN or above turns the audit off even when
 * {@code enabled=true}. A status line in the main server log states whether the audit is active.
 *
 * <p>Field semantics and scope rules:
 * <ul>
 *   <li>{@code watch=true|false} is the watch flag the client sent, not whether a watch was set.
 *   A getData that fails with NONODE or NOAUTH still logs {@code watch=true}.</li>
 *   <li>Child watches and {@code NodeChildrenChanged} events carry the parent's path. With
 *   {@code pathPrefixes=/app/x}, a child watch on {@code /app} and its {@code NodeChildrenChanged}
 *   events are in scope, because they report creation and deletion of {@code /app/x}.</li>
 *   <li>Persistent recursive watches on an ancestor of a prefix are in scope.</li>
 *   <li>A failed request whose path is unknown, such as a SESSIONMOVED rejection or a malformed
 *   request, is always logged, whatever the path filter.</li>
 *   <li>When a multi-path request fails as a whole, each of its paths is logged with the
 *   request's error, because the client received nothing.</li>
 * </ul>
 */
public final class ReadAuditLogger {

    public static final String LOGGER_NAME = "org.apache.zookeeper.ReadAuditLogger";
    public static final String ENABLED_PROPERTY = "zookeeper.readAuditLog.enabled";
    public static final String PATH_PREFIXES_PROPERTY = "zookeeper.readAuditLog.pathPrefixes";
    public static final String AGGREGATE_WINDOW_MS_PROPERTY = "zookeeper.readAuditLog.aggregateWindowMs";
    public static final String AGGREGATE_BY_PROPERTY = "zookeeper.readAuditLog.aggregateBy";
    public static final String AGGREGATE_MAX_KEYS_PROPERTY = "zookeeper.readAuditLog.aggregateMaxKeys";

    private static final Logger LOG = LoggerFactory.getLogger(LOGGER_NAME);
    private static final Logger INTERNAL_LOG = LoggerFactory.getLogger(ReadAuditLogger.class);

    public static final String WATCH_TRUE = "watch=true";
    public static final String WATCH_FALSE = "watch=false";
    static final String WATCH_TYPE_DATA = "watchType=data";
    static final String WATCH_TYPE_EXIST = "watchType=exist";
    static final String WATCH_TYPE_CHILD = "watchType=child";
    static final String WATCH_TYPE_PERSISTENT = "watchType=persistent";
    static final String WATCH_TYPE_PERSISTENT_RECURSIVE = "watchType=persistentRecursive";
    private static final Map<WatcherType, String> WATCHER_TYPE_DETAILS = new EnumMap<>(WatcherType.class);
    private static final Map<WatcherMode, String> WATCH_MODE_DETAILS = new EnumMap<>(WatcherMode.class);

    static {
        for (WatcherType t : WatcherType.values()) {
            WATCHER_TYPE_DETAILS.put(t, "watcherType=" + t);
        }
        for (WatcherMode m : WatcherMode.values()) {
            WATCH_MODE_DETAILS.put(m, "watchMode=" + m);
        }
    }

    /** How far beyond the configured subtrees a path counts as in scope. */
    enum Scope {
        /** The prefix itself or a descendant. */
        SUBTREE,
        /** Also strict ancestors: a persistent recursive watch there covers the subtree. */
        SUBTREE_OR_ANCESTOR,
        /** Also the direct parent: its child watches report creation and deletion of the prefix. */
        SUBTREE_OR_PARENT
    }

    /** Grouping entity for aggregation. */
    public enum AggregateBy { SESSION, USER }

    /** One path-level audit entry, used by requests that touch several paths. */
    public static final class Entry {
        final String op;
        final String path;
        final String detail;
        final Code result;

        public Entry(String op, String path, String detail, Code result) {
            this.op = op;
            this.path = path;
            this.detail = detail;
            this.result = result;
        }
    }

    private static final class Config {
        final boolean enabled;
        final List<String> prefixes;
        final long windowMs;
        final AggregateBy aggregateBy;
        final int maxKeys;

        Config(boolean enabled, List<String> prefixes, long windowMs, AggregateBy aggregateBy, int maxKeys) {
            this.enabled = enabled;
            this.prefixes = prefixes;
            this.windowMs = windowMs;
            this.aggregateBy = aggregateBy;
            this.maxKeys = maxKeys;
        }
    }

    /** Aggregation key: everything that must be equal for two events to be folded together. */
    private static final class Key {
        final String event;
        final String entity;
        final String op;
        final String path;
        final String detail;
        final String result;
        final int hash;

        Key(String event, String entity, String op, String path, String detail, String result) {
            this.event = event;
            this.entity = entity;
            this.op = op;
            this.path = path;
            this.detail = detail;
            this.result = result;
            this.hash = Objects.hash(event, entity, op, path, detail, result);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Key)) {
                return false;
            }
            Key k = (Key) o;
            return hash == k.hash && event.equals(k.event) && entity.equals(k.entity)
                    && Objects.equals(op, k.op) && Objects.equals(path, k.path)
                    && Objects.equals(detail, k.detail) && Objects.equals(result, k.result);
        }
    }

    private static final class Counter {
        final LongAdder count = new LongAdder();
        final long first;
        final AtomicLong last;

        Counter(long now) {
            first = now;
            last = new AtomicLong(now);
        }
    }

    private static volatile Config config = fromSystemProperties();

    // Writers share the read lock; flush takes the write lock only to swap the map, so no count is lost.
    private static final ReadWriteLock SWAP_LOCK = new ReentrantReadWriteLock();
    private static volatile ConcurrentHashMap<Key, Counter> window = new ConcurrentHashMap<>();
    private static final AtomicLong OVERFLOW = new AtomicLong();
    private static volatile long windowStart = System.currentTimeMillis();
    private static ScheduledExecutorService flusher;

    static {
        logStatus(config);
        startFlusher(config);
    }

    private ReadAuditLogger() {
    }

    /**
     * Loads the configuration and logs whether the audit is active. Called when the request
     * pipeline starts, after zoo.cfg has been parsed, so the status line appears at startup.
     */
    public static void init() {
        // class initialization does the work
    }

    /** Detail for a request's watch flag. Returns a constant, so building it never allocates. */
    public static String watchDetail(boolean watch) {
        return watch ? WATCH_TRUE : WATCH_FALSE;
    }

    /** Detail for checkWatches and removeWatches. Returns a constant. */
    public static String watcherTypeDetail(WatcherType type) {
        return WATCHER_TYPE_DETAILS.get(type);
    }

    /** Detail for addWatch. Returns a constant. */
    public static String watchModeDetail(int zooDefMode) {
        return WATCH_MODE_DETAILS.get(WatcherMode.fromZooDef(zooDefMode));
    }

    // ---------------------------------------------------------------- configuration

    private static Config fromSystemProperties() {
        AggregateBy by;
        try {
            by = AggregateBy.valueOf(System.getProperty(AGGREGATE_BY_PROPERTY, "session").trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            INTERNAL_LOG.warn("Invalid {}, using session", AGGREGATE_BY_PROPERTY);
            by = AggregateBy.SESSION;
        }
        return new Config(
                Boolean.getBoolean(ENABLED_PROPERTY),
                parsePrefixes(System.getProperty(PATH_PREFIXES_PROPERTY, "")),
                Long.getLong(AGGREGATE_WINDOW_MS_PROPERTY, 0L),
                by,
                Integer.getInteger(AGGREGATE_MAX_KEYS_PROPERTY, 10000));
    }

    static List<String> parsePrefixes(String value) {
        List<String> result = new ArrayList<>();
        if (value == null) {
            return result;
        }
        for (String p : value.split(",")) {
            p = p.trim();
            if (p.isEmpty()) {
                continue;
            }
            if (!p.startsWith("/")) {
                INTERNAL_LOG.warn("Ignoring read audit path prefix '{}': must start with /", p);
                continue;
            }
            while (p.length() > 1 && p.endsWith("/")) {
                p = p.substring(0, p.length() - 1);
            }
            result.add(p);
        }
        return Collections.unmodifiableList(result);
    }

    /** Replaces the configuration. Flushes what the old configuration aggregated. Visible for testing. */
    static synchronized void configure(boolean enabled, String prefixes, long windowMs, AggregateBy by, int maxKeys) {
        stopFlusher();
        flush();
        config = new Config(enabled, parsePrefixes(prefixes), windowMs, by, maxKeys);
        logStatus(config);
        startFlusher(config);
    }

    private static void logStatus(Config c) {
        if (!c.enabled) {
            INTERNAL_LOG.info("Read audit log disabled. Set {}=true to enable it.", ENABLED_PROPERTY);
        } else if (!LOG.isInfoEnabled()) {
            INTERNAL_LOG.warn("Read audit log is enabled but logger {} is not at INFO, so nothing will be logged."
                    + " Set that logger to INFO in the logback configuration.", LOGGER_NAME);
        } else {
            INTERNAL_LOG.info("Read audit log active: logger={} pathPrefixes={} aggregateWindowMs={} aggregateBy={}"
                    + " aggregateMaxKeys={}", LOGGER_NAME, c.prefixes.isEmpty() ? "all" : c.prefixes,
                    c.windowMs, c.aggregateBy, c.maxKeys);
        }
    }

    private static synchronized void startFlusher(Config c) {
        if (c.enabled && c.windowMs > 0) {
            flusher = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "ReadAuditLogger-flusher");
                t.setDaemon(true);
                return t;
            });
            flusher.scheduleAtFixedRate(ReadAuditLogger::flushSafely, c.windowMs, c.windowMs, TimeUnit.MILLISECONDS);
        }
    }

    private static synchronized void stopFlusher() {
        if (flusher != null) {
            flusher.shutdown();
            try {
                // let an in-flight flush finish so its lines are not interleaved with the final flush
                if (!flusher.awaitTermination(5, TimeUnit.SECONDS)) {
                    flusher.shutdownNow();
                }
            } catch (InterruptedException e) {
                flusher.shutdownNow();
                Thread.currentThread().interrupt();
            }
            flusher = null;
        }
    }

    public static boolean isEnabled() {
        return config.enabled && LOG.isInfoEnabled();
    }

    /** True if the path is one of the configured znodes or a descendant of one. */
    static boolean pathMatches(List<String> prefixes, String path) {
        if (prefixes.isEmpty()) {
            return true;
        }
        if (path == null) {
            return false;
        }
        for (String p : prefixes) {
            if (p.equals("/") || path.equals(p) || (path.startsWith(p) && path.charAt(p.length()) == '/')) {
                return true;
            }
        }
        return false;
    }

    /** True if the path is the direct parent of a configured znode, e.g. /app for /app/x. */
    static boolean pathIsParent(List<String> prefixes, String path) {
        if (path == null) {
            return false;
        }
        for (String p : prefixes) {
            int slash = p.lastIndexOf('/');
            if (slash >= 0 && !p.equals("/") && path.equals(slash == 0 ? "/" : p.substring(0, slash))) {
                return true;
            }
        }
        return false;
    }

    static boolean inScope(List<String> prefixes, String path, Scope scope) {
        if (pathMatches(prefixes, path)) {
            return true;
        }
        switch (scope) {
        case SUBTREE_OR_ANCESTOR:
            return pathIsAncestor(prefixes, path);
        case SUBTREE_OR_PARENT:
            return pathIsParent(prefixes, path);
        default:
            return false;
        }
    }

    /** Scope of a single-path request, from its type and watch detail. */
    private static Scope requestScope(int type, String detail) {
        switch (type) {
        case OpCode.addWatch:
            if (WATCH_MODE_DETAILS.get(WatcherMode.PERSISTENT_RECURSIVE).equals(detail)) {
                return Scope.SUBTREE_OR_ANCESTOR;
            }
            return WATCH_MODE_DETAILS.get(WatcherMode.PERSISTENT).equals(detail) ? Scope.SUBTREE_OR_PARENT : Scope.SUBTREE;
        case OpCode.getChildren:
        case OpCode.getChildren2:
        case OpCode.getChildrenPaginated:
            return WATCH_TRUE.equals(detail) ? Scope.SUBTREE_OR_PARENT : Scope.SUBTREE;
        default:
            return Scope.SUBTREE;
        }
    }

    /** Scope of one re-registered watch from setWatches. */
    private static Scope entryScope(String detail) {
        if (WATCH_TYPE_PERSISTENT_RECURSIVE.equals(detail)) {
            return Scope.SUBTREE_OR_ANCESTOR;
        }
        if (WATCH_TYPE_CHILD.equals(detail) || WATCH_TYPE_PERSISTENT.equals(detail)) {
            return Scope.SUBTREE_OR_PARENT;
        }
        return Scope.SUBTREE;
    }

    /** True if the path is a strict ancestor of a configured znode, e.g. a recursive watch on / covers /app. */
    static boolean pathIsAncestor(List<String> prefixes, String path) {
        if (path == null) {
            return false;
        }
        String withSlash = path.equals("/") ? "/" : path + "/";
        for (String p : prefixes) {
            if (p.startsWith(withSlash)) {
                return true;
            }
        }
        return false;
    }

    // ---------------------------------------------------------------- entry points

    /** Returns true for request types that read state or manage watches. */
    public static boolean isReadOrWatchOp(int type) {
        switch (type) {
        case OpCode.exists:
        case OpCode.getData:
        case OpCode.getACL:
        case OpCode.getChildren:
        case OpCode.getChildren2:
        case OpCode.getChildrenPaginated:
        case OpCode.getAllChildrenNumber:
        case OpCode.getEphemerals:
        case OpCode.multiRead:
        case OpCode.setWatches:
        case OpCode.setWatches2:
        case OpCode.addWatch:
        case OpCode.checkWatches:
        case OpCode.removeWatches:
            return true;
        default:
            return false;
        }
    }

    /**
     * Logs one completed read or watch-management request.
     *
     * @param entries per-path entries for requests touching several paths; when non-null they are
     *                logged instead of the single {@code path}/{@code detail}
     */
    public static void logRequest(Request request, ServerCnxn cnxn, String path, String detail,
                                  List<Entry> entries, Code err) {
        Config c = config;
        if (!c.enabled || !LOG.isInfoEnabled() || !isReadOrWatchOp(request.type)) {
            return;
        }
        if (entries == null || (entries.isEmpty() && err != Code.OK)) {
            // a failure with no known path, e.g. SESSIONMOVED or a malformed request, is always logged
            boolean unknownPathFailure = path == null && err != Code.OK;
            if (!unknownPathFailure && !inScope(c.prefixes, path, requestScope(request.type, detail))) {
                return;
            }
            record(c, "request", entity(c, request.sessionId, cnxn, request.getUsers()),
                    opName(request.type), path, detail, err.name());
            return;
        }
        String entity = null;
        for (Entry e : entries) {
            if (!inScope(c.prefixes, e.path, entryScope(e.detail))) {
                continue;
            }
            if (entity == null) {
                entity = entity(c, request.sessionId, cnxn, request.getUsers());
            }
            // when the whole request failed the client received nothing, so every path gets that error
            Code result = err != Code.OK || e.result == null ? err : e.result;
            record(c, "request", entity, e.op, e.path, e.detail, result.name());
        }
    }

    /** Builds one entry per re-registered watch so each path is filtered and aggregated on its own. */
    public static List<Entry> setWatchesEntries(String op, List<String> data, List<String> exist, List<String> child,
                                                List<String> persistent, List<String> persistentRecursive) {
        List<Entry> entries = new ArrayList<>();
        addWatchEntries(entries, op, WATCH_TYPE_DATA, data);
        addWatchEntries(entries, op, WATCH_TYPE_EXIST, exist);
        addWatchEntries(entries, op, WATCH_TYPE_CHILD, child);
        addWatchEntries(entries, op, WATCH_TYPE_PERSISTENT, persistent);
        addWatchEntries(entries, op, WATCH_TYPE_PERSISTENT_RECURSIVE, persistentRecursive);
        return entries;
    }

    private static void addWatchEntries(List<Entry> entries, String op, String detail, List<String> paths) {
        if (paths != null) {
            for (String p : paths) {
                entries.add(new Entry(op, p, detail, null));
            }
        }
    }

    /** Logs a watch notification being delivered to a client connection. */
    public static void logWatchFired(ServerCnxn cnxn, WatchedEvent event) {
        Config c = config;
        if (!c.enabled || !LOG.isInfoEnabled()) {
            return;
        }
        // NodeChildrenChanged carries the parent's path; on the direct parent it reports the prefix's creation/deletion
        Scope scope = event.getType() == Watcher.Event.EventType.NodeChildrenChanged ? Scope.SUBTREE_OR_PARENT : Scope.SUBTREE;
        if (!inScope(c.prefixes, event.getPath(), scope)) {
            return;
        }
        StringBuilder users = new StringBuilder();
        for (Id id : cnxn.getAuthInfo()) {
            String user = AuthUtil.getUser(id);
            if (user != null) {
                users.append(users.length() == 0 ? "" : ",").append(user);
            }
        }
        record(c, "watchFired", entity(c, cnxn.getSessionId(), cnxn, users.toString()), null, event.getPath(),
                "type=" + event.getType(), null);
    }

    // ---------------------------------------------------------------- recording and aggregation

    private static void record(Config c, String event, String entity, String op, String path, String detail, String result) {
        if (c.windowMs <= 0) {
            LOG.info(format(event, entity, op, path, detail, result, null));
            return;
        }
        Key key = new Key(event, entity, op, path, detail, result);
        long now = System.currentTimeMillis();
        SWAP_LOCK.readLock().lock();
        try {
            ConcurrentHashMap<Key, Counter> w = window;
            Counter counter = w.get(key);
            if (counter == null) {
                if (w.size() >= c.maxKeys) {
                    // Window is full of distinct groups: keep the audit complete by logging this one as is.
                    OVERFLOW.incrementAndGet();
                    LOG.info(format(event, entity, op, path, detail, result, null));
                    return;
                }
                counter = w.computeIfAbsent(key, k -> new Counter(now));
            }
            counter.count.increment();
            counter.last.accumulateAndGet(now, Math::max);
        } finally {
            SWAP_LOCK.readLock().unlock();
        }
    }

    private static void flushSafely() {
        try {
            flush();
        } catch (Throwable t) {
            INTERNAL_LOG.warn("Failed to flush read audit aggregates", t);
        }
    }

    /** Emits one line per aggregated group and starts a new window. Visible for testing. */
    static void flush() {
        ConcurrentHashMap<Key, Counter> old;
        long overflow;
        long start;
        long end = System.currentTimeMillis();
        SWAP_LOCK.writeLock().lock();
        try {
            old = window;
            window = new ConcurrentHashMap<>();
            overflow = OVERFLOW.getAndSet(0);
            start = windowStart;
            windowStart = end;
        } finally {
            SWAP_LOCK.writeLock().unlock();
        }
        for (Map.Entry<Key, Counter> e : old.entrySet()) {
            Key k = e.getKey();
            Counter v = e.getValue();
            String agg = "count=" + v.count.sum()
                    + " first=" + Instant.ofEpochMilli(v.first)
                    + " last=" + Instant.ofEpochMilli(v.last.get());
            LOG.info(format("aggregate source=" + k.event, k.entity, k.op, k.path, k.detail, k.result, agg));
        }
        if (overflow > 0) {
            LOG.info("event=aggregateOverflow windowStart={} windowEnd={} individuallyLogged={}",
                    Instant.ofEpochMilli(start), Instant.ofEpochMilli(end), overflow);
        }
    }

    // ---------------------------------------------------------------- formatting

    private static String entity(Config c, long sessionId, ServerCnxn cnxn, String users) {
        InetSocketAddress addr = cnxn == null ? null : cnxn.getRemoteSocketAddress();
        String ip = addr == null ? "unknown" : addr.getAddress().getHostAddress();
        if (c.aggregateBy == AggregateBy.USER && c.windowMs > 0) {
            return "user=" + users + " clientIp=" + ip;
        }
        String client = addr == null ? "unknown" : ip + ":" + addr.getPort();
        return "session=0x" + Long.toHexString(sessionId) + " client=" + client + " user=" + users;
    }

    private static String format(String event, String entity, String op, String path, String detail,
                                 String result, String aggregate) {
        StringBuilder sb = new StringBuilder(160);
        sb.append("event=").append(event).append(' ').append(entity);
        if (op != null) {
            sb.append(" op=").append(op);
        }
        sb.append(" path=").append(path == null ? "-" : path);
        if (detail != null) {
            sb.append(' ').append(detail);
        }
        if (result != null) {
            sb.append(" result=").append(result);
        }
        if (aggregate != null) {
            sb.append(' ').append(aggregate);
        }
        return sb.toString();
    }

    private static String opName(int type) {
        switch (type) {
        case OpCode.addWatch:
            return "addWatch";
        case OpCode.getChildrenPaginated:
            return "getChildrenPaginated";
        default:
            return Request.op2String(type);
        }
    }
}
