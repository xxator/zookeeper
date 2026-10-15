<!--
Copyright 2002-2022 The Apache Software Foundation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
//-->

# Read Audit Log: Design Choices

This document records the design decisions behind the read audit log, the alternatives that were
considered, and why each choice was made. The implementation lives in `ReadAuditLogger`, with hooks
in `FinalRequestProcessor`. The metrics live in `ServerMetrics`.

## Goal

Identify every entity that reads from ZooKeeper: which session, client address and authenticated
user read or watched which znode, and with what result. Writes are already covered by the existing
audit log (`ZKAuditProvider`), so they are out of scope.

Two constraints shaped almost every decision:

- **Reads must never be slowed by the audit log**, even when the disk is slow or stalled.
- **Read volume can be very high**, so the log has to be scoped and compressible.

## 1. Where the hook lives

**Choice.** One call at the end of `FinalRequestProcessor.processRequest`, after the request has
been served and its result code is known, plus one call in the `SessionMovedException` handler.

**Why.** Every read and watch request passes through this one method on every kind of server:
standalone, leader, follower and observer. At this point the path, the watch flag and the result
code are all known, so a single line can describe the whole request, including failures such as
NONODE and NOAUTH.

**Alternatives considered.**

- **Extend the existing write audit (`ZKAuditProvider`).** Rejected. That audit is driven by
  transaction results, and reads have no transaction. It also shares one logger with writes, so
  read volume could not be routed or sized separately.
- **Hook in the connection classes (`NIOServerCnxn`, `NettyServerCnxn`).** Rejected for requests.
  The request is not yet decoded there, and the result is not known.
- **Hook in `PrepRequestProcessor`.** Rejected. On followers and observers, reads never pass through
  it, and the result is not known yet.

**Consequence.** Each server logs only the reads it serves. Followers and observers answer reads
locally, so a complete picture needs the log from every server in the ensemble.

## 2. What is logged

**Choice.** One line per read or watch-management request:

- reads: `getData`, `exists`, `getChildren`, `getChildren2`, `getChildrenPaginated`, `getACL`,
  `getAllChildrenNumber`, `getEphemerals`, `multiRead`;
- watch management: `addWatch`, `setWatches`, `setWatches2`, `checkWatches`, `removeWatches`.

Requests that touch several paths are split into one entry per path. A `multiRead` gets one line per
sub-operation, each with its own result, and `setWatches` gets one line per re-registered watch.
Splitting is what allows each path to be filtered and aggregated on its own.

**Field semantics that needed a decision.**

- **`watch=true|false` is the flag the client sent**, not whether a watch was set. A `getData` that
  fails with NONODE still logs `watch=true`. Recording whether a watch was actually registered
  would need a hook inside the watch managers, which section 5 argues against.
- **A failed request whose path is unknown is always logged with `path=-`**, whatever the path
  filter. Examples are a SESSIONMOVED rejection, a malformed request, and a `multiRead` whose first
  sub-operation is unsupported. An auditor wants these, and without a path they cannot be filtered.
- **When a multi-path request fails as a whole, every path is logged with the request's error**,
  because the client received nothing.
- **Writes are never logged here.**

## 3. Configuration

**Choice.** System properties, read once when the request pipeline starts. Any key that `zoo.cfg`
does not recognize becomes a system property with `zookeeper.` in front, so the same settings work
from `zoo.cfg` (without the prefix) or as JVM flags. The audit is off by default.

**Alternatives considered.**

- **Runtime reconfiguration through JMX.** Not required. Changing settings needs a restart. The
  internal `configure` method already supports a swap, so a JMX bean could be added later.

**Supporting decisions.**

- **A status line at startup** in the main server log says whether the audit is active and with
  which settings. It becomes a WARN when the audit is enabled but its logger is not at INFO, because
  routing the logger at WARN silently turns the feature off.
- **The disabled path allocates nothing.** Detail strings such as `watch=true` are precomputed
  constants, and per-path entry lists are only built when the audit is enabled.

## 4. Path filter

**Choice.** `readAuditLog.pathPrefixes` takes a comma-separated list of znodes. A request is in scope
when its path is a prefix or below it, compared as path text: `/app` matches `/app/x/y` but not
`/application`. Two widenings apply to watch registrations:

- **Persistent recursive watches on any ancestor** of a prefix are in scope, because they cover the
  prefix's subtree.
- **Child watches on the direct parent** of a prefix are in scope, because that is how a client learns
  the prefix was created or deleted. A plain `getChildren` of the parent without a watch is not.

**Alternatives considered.**

- **Match on the znode tree, like an ACL check.** Rejected. It costs a tree lookup per request and
  does not change what is exposed.
- **Also log listing and counting requests on every ancestor**, so `ls /` is logged under a `/app`
  filter. Rejected as too noisy: every `ls /` from every client would be logged.

**Accepted gap.** `ls /` reveals the name `app` without being logged, and `getAllChildrenNumber /`
counts znodes under `/app`. Only names and counts escape this way. Reading data or ACLs, or setting
a watch, requires a path that is logged.

## 5. Watch notifications are not logged

This went through three designs.

1. **One line per watcher, from the connection classes.** A write reaching 300 watchers made 300
   logging calls on the thread applying the write. A review measured a single synchronous file
   write holding that thread for 49 ms during a slow-disk period.
2. **One summary line per triggered event, listing every reader it reached**, hooked into the watch
   managers and `DataTree.setWatches`. That removed the per-watcher appender calls but still put work
   on the write path and changed three core files.
3. **No notification logging.** This is the current design.

**Why notifications add little.** Every watch starts with a logged request: a read with
`watch=true`, `addWatch`, or `setWatches` after a reconnect, including to a different server. A
notification carries only an event type and a path, never data. To see the new value the client
must read again, and that read is logged. So the request lines already identify every reader.

**What is given up.**

- **Znode names reported by persistent recursive watches.** A client holding a recursive watch on
  `/app` is told about every znode created or deleted below it without sending a read. Its `addWatch`
  line identifies it, but not which names it learned.
- **When each notification was delivered**, and how many clients a change reached.

**Why pre-existing watches are still covered.** Enabling the audit requires a restart, which makes
every client reconnect and re-register its watches with `setWatches`, and those are logged.

**What is gained.** No audit work on the write path, and no changes to the watch managers or
`DataTree`, which were the parts most likely to draw objections upstream.

## 6. Aggregation

**Choice.** Optional. With `readAuditLog.aggregateWindowMs` above zero, identical events from the
same entity are counted, and one `event=aggregate` line per group is written per window with `count`,
`first` and `last`. Events are identical when the entity, operation, path, detail and result all
match, so failures such as NONODE stay separate from successful reads.

- **Entity:** `session` by default (session ID, client address and users), or `user` (users and
  client IP, merging that client's sessions).
- **Memory bound:** `readAuditLog.aggregateMaxKeys`, default 10000 groups per window. Events past it
  are logged individually rather than folded, and an `event=aggregateOverflow` line reports how
  many.
- **Concurrency:** request threads share a read lock while updating counters. The flush thread takes
  the write lock only to swap in a new map, then writes the old one outside the lock. A test with 8
  clients and 4000 reads, while windows flush and the bound overflows, counts exactly 4000.

**Alternative considered.** Dropping events past the group bound. Rejected in favor of logging them
individually, so the bound limits memory without losing audit lines.

**Accepted gaps.** Aggregate lines lose the order of reads relative to writes within a window, and
the open window is lost on shutdown (section 9).

## 7. Never blocking requests: queue-full policy

When the disk cannot keep up, a logger can block, buffer without limit, or drop.

| Option | Effect |
|---|---|
| Block, like the HDFS audit log | Keeps the log complete, but a long stall eventually fills any bounded buffer, and then every read, and every write queued behind it, waits on the disk. |
| Buffer without limit | Memory grows until the server runs out. Worse than slow reads. |
| **Drop and count (chosen)** | Reads and writes never wait. Lines that do not fit are dropped, and every drop is counted and reported. |

**Choice of mechanism.** The logger owns a bounded queue and one daemon writer thread. Request
threads format the line and offer it to the queue, which never waits. The writer hands lines to
Logback in order. When the queue is full the line is dropped and counted, and the writer logs
`event=dropped count=N since=... until=...` as soon as it catches up, and at least once a second while
drops continue. The writer writes that report itself, so the report cannot be dropped.

**Alternatives considered.**

- **Logback's async appender, blocking.** Request threads still block once its buffer fills.
- **Logback's async appender, non-blocking (`neverBlock=true`).** Drops silently, with no count.
  Whether requests could wait at all also depended on the operator choosing the right Logback setup.
  Its default one-second `maxFlushTime` also discarded queued lines on stop.
- **Leave it to Logback configuration.** Rejected for the same reason: the guarantee should not depend
  on configuration. With the built-in queue, a plain file appender is the recommended setup, and
  wrapping it in an async appender only adds a second buffer.

**Queue size.** `readAuditLog.queueSize`, default 10000 lines, about 2 MB of heap when full. 65536 was
considered and rejected to save memory, at the cost of dropping sooner during a stall. One
aggregation flush can produce up to `aggregateMaxKeys` lines, also 10000 by default, so a flush
arriving on top of a backlog can drop some summary lines. They are counted and reported.

**Disk placement.** Put the audit file on a different disk from ZooKeeper's transaction log
(`dataLogDir`). Requests no longer wait on the audit file, but sharing a disk with the transaction
log can still slow its fsyncs and raise drop counts.

## 8. Timestamps

**Choice.** A line's timestamp is the one Logback adds when the writer thread writes it. Normally that
is within a millisecond of the request. During a backlog it can lag by seconds.

**Alternative considered.** An `at=` field with the request time on every line, captured before
queuing. Not adopted: the Logback time is considered sufficient. Aggregate lines carry their own
`first=` and `last=` request times either way.

## 9. Shutdown

**Choice.** No flush on shutdown. Lines still in the queue and the open aggregation window are lost
when the process exits, including on a normal `zkServer.sh stop`. Normally the queue is near empty.
A shorter aggregation window limits what can be lost.

**Alternative considered.** Draining the queue and flushing the window during server shutdown, about
15 lines of code. Not adopted.

## 10. Metrics

Three server metrics, in `mntr` and the configured metrics provider. Nothing is recorded while the
audit is disabled.

| Metric | Type | Meaning |
|---|---|---|
| `read_audit_lines` | Counter | Lines the writer handed to Logback, including aggregate and overflow lines. `event=dropped` reports are not counted. |
| `read_audit_dropped` | Counter | Lines dropped because the queue was full. `lines + dropped` is every line produced. |
| `read_audit_hook_time_us` | Basic summary: min, avg, max, cnt, sum | Time request threads spend in the hook: filtering, formatting and queuing. One sample per read or watch request, including requests the filter skips. The writer and flush threads are not timed. |

**Alternative considered.** Timing the notification hook as well. Moot once notifications stopped
being audited, so a watch delivery records nothing.

## 11. Other alternatives not adopted

- **A `zxid=` field on read lines.** Writes are never applied while reads are in flight, so the
  server's last processed zxid is exactly the data version a read saw. That would place every read
  between writes. Not required.

## Testing approach

- **Integration tests** run every read and watch operation against a real server, on both the NIO and
  Netty stacks, and a five-server quorum confirms that a follower and an observer each log the reads
  they serve, exactly once.
- **Mocked-processor tests** reach failure paths that a normal client cannot produce: SESSIONMOVED,
  unsupported and malformed multi-reads, and a stalled disk. The stalled-disk test blocks a Logback
  appender, sends 20 reads with a 5-line queue, and checks that the reads do not wait, 6 lines are
  written, and one `event=dropped count=14` line follows.
- **`ReadAuditLoggerBenchmark`** is a load test outside the regular suite, for fan-out, reads, and
  mixed load across audit off, one line per event, and aggregation.
