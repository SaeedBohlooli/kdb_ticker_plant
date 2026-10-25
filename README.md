# kdb_ticker_plant

A real-time market-data capture system built with kdb+/q, based on KX's standard **kdb+tick** architecture: a feed publishes trades and quotes to a tickerplant, which logs every message to disk and forwards it to subscribers such as the real-time database (RDB).

## Quick start

Run each process in its own terminal, from the repo root, in this order:

```
q tick.q sym tick/tplogs -p 5010      / 1. tickerplant (TP)
q tick/r.q :5010 -p 5011              / 2. real-time database (RDB)
q tick/feed.q                         / 3. feed
```

Check that data is arriving, in the RDB window:

```q
count trade
-5#quote
select last price by sym from trade
```

`tick.q` arguments: `q tick.q SRC DST [-p port] [-t ms]`
- `SRC` is the schema name: `sym` loads `tick/sym.q`.
- `DST` is the folder for the log file. If you leave it out, logging is disabled.
- `-t ms` enables batch mode (see [TP modes](#tickerplant-modes-real-time-vs-batch)).

## Architecture

### What it does

The system has four jobs:

1. **Capture** every market-data message without losing any, even after a crash.
2. **Distribute** each message to interested processes straight away.
3. **Store** today's data in memory for fast queries.
4. **Archive** it to disk at end of day for historical queries.

### Components

Each component is a separate q process listening on its own port:

```
                       ┌────────────────────────────┐
  ┌───────────┐ .u.upd │   TICKERPLANT  (TP)        │  upd   ┌──────────────────┐
  │  FEED     │───────►│   tick.q + tick/u.q        │───────►│ RDB (real-time)  │
  │ feed.q    │ async  │   port 5010                │  pub   │ tick/r.q  :5011  │
  └───────────┘        │  • log every message       │        │ today's data, in │
                       │  • publish to subscribers  │        │ memory (tables)  │
                       └─────────────┬──────────────┘        └────────┬─────────┘
                                     │ append                         │ end of day:
                                     ▼                                │ save to disk
                       ┌────────────────────────────┐        ┌────────▼─────────┐
                       │  LOG FILE                  │        │ HDB (historical) │
                       │  tick/tplogs/sym2026.10.25 │        │ port 5012        │
                       │  one file per day          │        │ partitioned by   │
                       └────────────────────────────┘        │ date, on disk    │
                                                             └──────────────────┘
```

| Process | File | Port | Job | Python analogy |
|---|---|---|---|---|
| **Feed** | `tick/feed.q` | none (client) | Generates fake trades and quotes. In production it reads from Bloomberg, Reuters or an exchange. | A producer script pushing events to a broker |
| **Tickerplant (TP)** | `tick.q` + `tick/u.q` | 5010 | Receives each message, appends it to a log file, publishes it to subscribers. **Stores nothing in memory.** | A Kafka broker with a write-ahead log |
| **RDB** | `tick/r.q` | 5011 | Subscribes to the TP and keeps **today's** data in memory | A service holding a growing pandas DataFrame |
| **HDB** | (not in this repo yet) | 5012 | Serves **past days'** data from disk | Parquet files partitioned by date |

`tick/sym.q` defines the table schemas (`trade`, `quote`) that the TP and the RDB both use.

### Message flow

**1. Feed → TP.** Every 100 ms the feed calls a function on the TP:

```q
h(`.u.upd;`trade;(times;syms;prices;sizes))
```

`h` is an IPC connection handle, similar to a socket. A list whose first item is a function name means "run this function on the remote process with these arguments", like an RPC call. `h` is **negative** (`neg hopen`), which makes the call **async**: the feed sends the message and doesn't wait for a reply.

**2. TP logs it.** The TP appends `` (`upd;`trade;data) `` to today's log file before doing anything else. This is the system's durability guarantee.

**3. TP publishes it.** The TP looks up who subscribed to `trade` (stored in `.u.w`) and sends each subscriber `` (`upd;`trade;data) ``, filtered to the symbols that subscriber asked for.

**4. RDB stores it.** On the RDB, `upd` is defined as `insert`, so the message becomes new rows in the in-memory `trade` table, ready to query.

### Design principles

**The TP does as little as possible.** It doesn't store or query anything; it only logs and forwards. Each message passes through quickly, so the TP never becomes a bottleneck, and slow queries on the RDB can't hold up incoming data.

**The log file makes recovery possible.** When the RDB starts, or restarts after a crash, it:
1. asks the TP for the table schemas and the log file location (`.u.sub`, `` `.u `i`L ``),
2. **replays** the log with `-11!`, re-running every `upd` from midnight until now,
3. continues receiving live data.

No data is lost. This is done by `.u.rep` in `tick/r.q`.

**Publish/subscribe with filtering.** Any number of processes can subscribe, each to particular tables and symbols:

```q
.u.sub[`trade;`AAPL`MSFT]    / only AAPL and MSFT trades
.u.sub[`;`]                  / all tables, all symbols (what the RDB does)
```

In production, other subscribers (real-time engines that calculate VWAP or detect signals, for example) connect to the same TP.

### Tickerplant modes: real-time vs batch

`tick.q` defines two versions of `upd`. Which one you get depends on whether the TP is started with `-t`:

| | **Real-time** (no `-t`) | **Batch** (`-t 1000`) |
|---|---|---|
| On each message | Log it and publish immediately | Log it and **buffer** it in a local table |
| Publishing | Every message | Once per timer tick (`.z.ts`), as a batch |
| Trade-off | Lowest latency | Higher throughput, delay of up to one tick |

In real-time mode the TP still sets a 1-second timer, but only to check whether the date has changed.

### End of day (EOD)

At midnight `.u.ts` detects the date change and calls `.u.endofday`, which:

1. sends `.u.end[date]` to every subscriber,
2. closes today's log file and opens a new one (for example `sym2026.10.26`).

When the RDB receives `.u.end`, it saves its tables to the HDB directory (`.Q.hdpf`), clears them from memory and tells the HDB to reload.

### Key q concepts used

| Concept | Meaning |
|---|---|
| `hopen` / `h` | Open an IPC connection; `h` is the handle |
| `neg h` | Async send: no reply, no waiting |
| `h"expr"` | Sync call: wait for the result |
| `.z.ts` | Timer callback, run every `\t` milliseconds |
| `.z.pc` | Called when a connection closes; used to remove dead subscribers |
| `-11!` | Replay a log file, re-running each message |
| `.u` namespace | All tickerplant functions and state (`.u.upd`, `.u.sub`, `.u.w`, ...) |

### Common interview questions

- **Why doesn't the TP store data?** To keep it fast. Querying and storage are the RDB's and HDB's jobs.
- **What happens if the RDB dies?** It restarts and replays the TP log with `-11!`, so nothing is lost.
- **What happens if the TP dies?** Messages sent while it's down are lost; the log holds everything up to the crash. Production systems add a buffer on the feed side or a second TP.
- **Real-time vs batch TP?** A trade-off between latency and throughput.
- **What's in the log file?** A list of `` (`upd;table;data) `` messages, which `-11!` replays.
- **RDB vs HDB?** Today's data in memory vs past days on disk, partitioned by date.

## Files

| File | Role |
|---|---|
| `tick.q` | Tickerplant entry point: loads the schema, opens the log, defines `upd` |
| `tick/u.q` | Pub/sub library: `.u.sub`, `.u.pub`, `.u.del`, `.u.end` |
| `tick/sym.q` | Table schemas for `trade` and `quote` |
| `tick/r.q` | Real-time database: subscribes, replays the log, handles end of day |
| `tick/feed.q` | Dummy feed that publishes random trades and quotes |
| `tick/tplogs/` | Tickerplant log files, one per day |

## File walkthrough

### `tick/sym.q`: table schemas

```q
quote:([]time:`timespan$(); sym:`g#`symbol$(); bid:`float$(); bsize:`long$(); ask:`float$(); asize:`long$())
trade:([]time:`timespan$(); sym:`g#`symbol$(); price:`float$(); size:`int$())
```

**What it does:** creates two empty tables with fixed column names and types, the system's `CREATE TABLE` statements. It contains no logic.

**Who uses it:** the tickerplant loads it at startup (`tick.q` line 5: `` system"l tick/",src,".q" ``; `sym` in `q tick.q sym ...` is this file's name). The RDB never loads it; it receives the empty tables from the TP when it subscribes, so the schema lives in one place.

#### Syntax

| Piece | Meaning |
|---|---|
| `trade: ...` | Assignment (`:` is q's `=`) |
| `([] c1:v1; c2:v2)` | A table. The empty `[]` means no key columns; a keyed table is `([k:...] c:...)`. `;` separates columns. |
| `()` | An empty, untyped list |
| `` `float$() `` | Cast (`$`) the empty list to float: an empty float column. Typed columns reject bad data instead of storing it. |
| `` `g# `` | Apply the *grouped* attribute: a hash index from each value to its row positions |

A q table is a list of columns, each column one contiguous array, like a pandas DataFrame. That's a big part of why q is fast.

#### Types used

| In sym.q | Type | Char | Number | Example | pandas |
|---|---|---|---|---|---|
| `` `timespan$() `` | timespan | `n` | 16 | `0D09:30:00.000000000` | `timedelta64[ns]` |
| `` `symbol$() `` | symbol | `s` | 11 | `` `AAPL `` | `category` |
| `` `float$() `` | float | `f` | 9 | `150.5` | `float64` |
| `` `long$() `` | long | `j` | 7 | `100` | `int64` |
| `` `int$() `` | int | `i` | 6 | `100i` | `int32` |

- A **negative** type number is an atom (one value) and a **positive** one is a list: `type 150.5` is `-9h`, while `` type `float$() `` is `9h`.
- Whole numbers default to **long**. Use the `i` suffix for int (`100i`) and `f` or a decimal point for float (`150f`, `150.0`).
- **Symbol vs string:** `` `AAPL `` is interned (stored once, compared as a number), so it's fast for repeated identifiers. `"AAPL"` is a list of characters, for free text.

#### Attributes

| Attribute | Name | Meaning | Typical use |
|---|---|---|---|
| `` `s# `` | sorted | Ascending order, so binary search works | Time columns, `tables[]` output |
| `` `u# `` | unique | All values distinct (hash lookup) | Keys |
| `` `p# `` | parted | Equal values stored together in blocks | `sym` on disk in the HDB |
| `` `g# `` | grouped | Hash index on any column | `sym` in the RDB |

#### Try it

```q
q)\l tick/sym.q
q)tables[]
`s#`quote`trade
q)meta trade                 / c=column t=type f=foreign key a=attribute
c    | t f a
-----| -----
time | n
sym  | s   g
price| f
size | i
q)`trade insert (0D09:30:00.000;`AAPL;150.5;100i)
q)`trade insert (0D09:30:00.000;`AAPL;150;100i)    / 150 is a long, price is float
'type
```

q will accept a long into an `int` column (`200` for `200i`), but not a long into a `float` column. Don't rely on this; send the exact type.

#### Key point: inserts go by position

q matches incoming data to columns **by position, not by name**. Every sender must use the same column order as `sym.q`. If `feed.q` sends `bid;ask;bsize;asize` but the table is `bid;bsize;ask;asize`, a float lands in the `long` column `bsize` and the RDB raises `'type`.

#### Practice

1. Add an `ohlc` table (`time`, `sym`, `open`, `high`, `low`, `close`), load the file and run `meta ohlc`.
2. What do `type 1 2 3` and `type 1.0 2.0` return?
3. Why is `sym` a symbol and not a string?
