# VARKA-260: a reference post, Spark's code generator by the numbers

*Row 260 of `m7/PLAN.md`, `m9/SCOPE.md` item 80. Planned and drafted on 16 October 2026 in the
shape of `VARKA-371.md`: the question, the claim, the evidence the record already holds, the
outline, what a reader can reproduce, and done-when. It goes out between rows 258 and 259 and the
next long post. The draft is `POST_CODEGEN_NUMBERS.md`; section 9.1 lists what is owed.*

## 1. The question

The reader runs or tunes Spark and has met one of the numbers below in a log line, a setting or one
of the earlier posts. They want one page that answers, for each number that decides what Spark
compiles and how well: **what is it, what happens past it, how would I see it, and what moves it?**

## 2. The claim, and the five-minute version

Nine numbers decide what Spark's code generator produces and how well the JVM runs it - 100 fields,
1024 characters, 255 parameter slots, 8000 bytes twice, 65535 as a default, 65,535 bytes and
entries, 100 cached classes and 16 bits of fast hash map - and most are internal settings or JVM
limits that the configuration page does not show. The five-minute version is the post's one table;
the sections under it give each number its measured consequence, where the record has one.

## 3. The evidence the post cites

* **Defaults, read from Spark 4.2.0's source at tag `v4.2.0`** on 16 October 2026:
  `spark.sql.codegen.maxFields` 100 (2.0.0), `spark.sql.codegen.methodSplitThreshold` 1024 (3.0.0),
  `spark.sql.codegen.hugeMethodLimit` 65535 (2.3.0), `spark.sql.codegen.cache.maxEntries` 100,
  static (2.4.0), `spark.sql.codegen.aggregate.fastHashMap.capacityBit` 16 in 10 to 30 (2.4.0),
  `spark.sql.codegen.broadcastCleanedSourceThreshold` -1 (4.0.0, not in the post's table);
  `CodeGenerator`'s `DEFAULT_JVM_HUGE_METHOD_LIMIT` 8000, `MAX_JVM_METHOD_PARAMS_LENGTH` 255,
  `MAX_JVM_CONSTANT_POOL_SIZE` 65535 and `calculateParamLength` (two slots for a `long` or a
  `double`, one more for a nullable value); the cache as a Guava-backed `NonFateSharingCache`
  logging `Code generated in ... ms` per compile; `HashAggregateExec`'s fall-through from a failed
  fast-map probe to the regular map.
* **8000, HugeMethodLimit**: `m6/POST_SPARK.md` section 1 (48 to 52 columns, about 1,500 to about
  9,200 ns a row, 6.2 times on JDK 25, stock 4.2.0) and section 4 (`hugeMethodLimit=8000`).
* **8000, DesiredMethodLimit**: `m6/POST_GIVEUPS.md` section 2 (50 cheap columns, 1,861 bytes,
  1.16 to 1.41 times slower in a stage on three machines; `maxFields` lowered to 98) and
  `m6/POST.md` section 2.
* **100 fields**: `m6/POST_SPARK.md` section 5 (the 101-column cached table, five times and two and
  a half times) and `m6/POST_GIVEUPS.md` section 2 (150 columns under `maxFields=200`, 12 to 22
  seconds two and a half to three and a half times slower).
* **1024 characters**: `m6/POST.md` section 1 (about seven characters a byte, 235-byte methods).
* **255 and 65,535**: `m6/POST_GIVEUPS.md` sections 3 to 5 (the INFO line, the two fallbacks, an
  eighth to a fifth, 3.1 to 4.4 times, about thirty places without a fallback) and the census of
  `m6/VARKA-188.md`.
* **100 classes**: `VARKA-258.md` section 2 (the cache keyed by the source, the literals in it).

## 4. The outline

1. **One paragraph and the table**: number, what it bounds, past it, how you see it, what moves it.
2. **The two limits of 8000.**
3. **100 fields, both ways.**
4. **1024 characters.**
5. **255 and 65,535.**
6. **100 classes**, linking row 258's post.
7. **16 bits.**
8. **How these were checked**, and one link to Varka.

## 5. What a reader can reproduce

* Each default: most of these settings are internal, so the post names the source files; on a
  running session `spark.conf.get("<key>")` prints the value in force.
* Each measured consequence: the demo scripts the linked posts publish (`method_size_cliff.scala`,
  `silent-giveups/compile_wait.scala`, `silent-giveups/log_level.scala`).

## 6. Done when

The post is published with every number tracing: each default to 4.2.0's source, each measurement
to a committed results file through `dev/varka_quote_check.py` or the published post that quotes
it; kept short.

## 7. Sequencing

1. This plan and the draft, and the row marked drafted.
2. The owed measurements of 9.1, or the owner's decision to publish with the two rows as the source
   describes them.
3. Publication between rows 258 and 259 and the next long post, then 9.2.

## 8. Explicitly out of this task

* New measurements beyond the two owed rows: the post is a reference to the record.
* Varka's own numbers, apart from the closing link.

## 9. Outcome

### 9.1 The draft, 16 October 2026

`POST_CODEGEN_NUMBERS.md` is the draft, about 1300 words on 4's outline. Seven of its nine rows
cite a measurement the record holds; two do not, and are written from the source with a bracketed
note. Owed before publication:

1. **Past the cache's 100 entries**: how many queries of a second loop of 150 distinct queries
   compile again. This is row 258's measurement 5 (`VARKA-258.md` 3), and the row quotes it when
   it lands.
2. **Past the fast hash map's capacity**: the cost of an aggregate whose distinct keys pass 2^16 and
   fall through to the regular map. Not measured anywhere in the record; a stock run, or the
   owner's decision to print the row from the source alone.
3. **Row 258's published link** for the 100-classes section.
4. **The owner's edit.**

### 9.3 The fast hash map's capacity, planned, 16 October 2026

**What.** 9.1's second owed row, on the stock 4.2.0 distribution under JDK 25:
`sql/varka/demo/hashmap/capacity.scala`, run by that directory's `run.sh` and by the
`hashmap-capacity` job of `varka-demo.yml` on a GitHub-hosted runner, in the pattern of row 258's
`compiles/times.scala`. The query is `SELECT k, sum(v) FROM t GROUP BY k` over 2^26 rows of
`RANGE` in one partition, `k = id % K`, for K = 16,384, 32,768, 61,440, 65,536, 65,537, 69,632,
131,072, 262,144 and 1,048,576, in three arms that take turns within each K: the default,
`capacityBit=20`, and the fast map off (`map.twolevel.enabled=false`). Two warm-up and five timed
runs per arm; best and median ns per row. The timed statement wraps the aggregate in the count of
its groups (which must equal K) and the sum of `hash(k, sum(v))`, which must agree across the arms.
2^26 rows keep a query's fixed costs (planning, the final aggregate of K groups, the regular map's
first page) small beside a run of a second or more, and still give each of a million keys 64 rows.
Why: the post's 16-bits section says what the source does and not what passing the capacity costs.

**The source, read at `v4.2.0` (32f72996011).** `HashAggregateExec.scala` 386-408: the fast map is
offered when every key and buffer field is primitive, decimal, string or interval, no buffer is a
byte-array decimal, and, under `spark.sql.codegen.aggregate.map.twolevel.partialOnly` (default
true), only to Partial and PartialMerge aggregates; so this query's partial aggregate gets it (a
`long` key, a `long` sum) and its final aggregate does not. 411-423 and 451-480: the map is
`RowBasedHashMapGenerator`'s unless `map.vectorized.enable` (default false, "testing/benchmarking
only") is set. 435-445: its capacity bit is `fastHashAggregateRowMaxCapacityBit`. 695-713: each
row calls the fast map's `findOrInsert` when its keys are not null, and a null result takes the
regular `UnsafeFixedWidthAggregationMap`, in the same generated code. `RowBasedHashMapGenerator`
58-68: `capacity = 1 << bit`, load factor 0.5, so `numBuckets = 2 * capacity`, `maxSteps = 2`;
145-183, `findOrInsert` returns null for a new key when its bucket and the next both hold other
keys, or when the empty bucket it found comes with `numRows >= capacity` or a full batch
(`FixedLengthRowBasedKeyValueBatch.appendRow` fails when its single page, `pageSizeBytes`, cannot
take another 40-byte record here). A null key never enters the fast map (`HashMapGenerator` 31).
Buckets never empty, so a key that fell through once falls through on every row.
`HashMapGenerator` 95-113: a single key hashes to the key plus `0x9e3779b9`, so dense keys take
consecutive buckets. `finishAggregate` (HashAggregateExec 197-213) reports the regular map's peak
memory only. The settings (`SQLConf.scala`): `spark.sql.codegen.aggregate.fastHashMap.capacityBit`,
16 in 10 to 30 (2.4.0); `spark.sql.codegen.aggregate.map.twolevel.enabled`, true (2.3.0);
`...twolevel.partialOnly`, true (3.2.1); all `buildConf`, internal and settable in a session.

**What is observed and what is derived.** Observed per arm: the generated stage holds the fast
map and the capacity the arm asked for (or none), no spill and no sort fallback, and the partial
aggregate's peak memory, which stays a fraction of a megabyte until the regular map takes its first
page (64 MB at the 4g driver), so it shows whether any key fell through. Derived from the source,
since Spark has no counter: with dense keys the fast map holds min(K, capacity) keys. The laptop
smoke grid (2^21 rows, counts only) showed the default arm's peak memory at 0.3 MB at 65,536 keys
and 64.2 MB at 65,537. The full grid ran to its end on the laptop in 6 minutes 20 seconds, its
checks passing; its times are not quoted.

**Prediction, registered before the run.** No step at 65,536. With `id % K` the first 65,536 keys
fill the fast map without a collision (`numRows < capacity` is checked before the increment, so the
65,536th fits) and every later key falls through, so the rows that take the regular map are the
fraction (K - 65,536) / K: none at 65,536, 1 in 65,537 at 65,537 (invisible in time), 6% at 69,632,
50% at 131,072, 75% at 262,144 and 94% at 1,048,576. The default arm's ns per row ramps between the
`capacityBit=20` arm and the off arm along that fraction; at 1,048,576 keys it is no faster than
the off arm, and slower by the failed probe (one or two bucket reads and key compares). Up to
65,536 keys the default and `capacityBit=20` arms run alike and faster than the off arm. Dense keys
are the fast map's best case: keys that collide fall through before the capacity under
`maxSteps = 2`, which this grid does not measure, and the post says so.
