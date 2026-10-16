# Nine numbers between your query and machine code

Widen a Spark SQL projection from 48 columns to 52 and it can run six times slower. Read every
column of a cached table that has 101 columns instead of 100 and it can take five times as long.
Neither raises an error. Each time, one number in Spark or in the JVM decided that your query
should take a slower path, and said so in a log line at best.

There are nine such numbers between a query plan and the machine code that runs it. They are
spread over Spark's settings, its code generator and HotSpot, and most are internal settings
that the configuration page doesn't list. This post puts them on one page, in the order a query
meets them.

![Nine numbers on the way to machine code](figures/svg/fig35-nine-numbers-on-the-way.svg)

*Figure 1. A Spark SQL stage on its way to machine code, with each limit under the step where it
acts. The defaults are Spark 4.2.0's and HotSpot's. Red marks the three that send a query down a
slower path without an error.*

## At the planner: 100 fields

Spark SQL compiles each stage of a query into one generated Java method. Before it does,
`spark.sql.codegen.maxFields` (default 100) decides who gets in. An operator with more than 100
fields is quietly left out of the stage. The only sign is in `EXPLAIN`: the operator has no `*(n)`
in front of it.

The same number decides a second thing that has nothing to do with operators. A cached table is
read in columnar batches only if every column is a primitive number or boolean and the whole
table, not just the columns the query reads, has at most 100 of them.

![A cached table of 100 and of 101 columns](figures/svg/fig14-the-cached-table-width.svg)

*Figure 2. Summing one column barely notices the 101st column; reading every column takes five
times as long once the table is read row by row. Raising `maxFields` to 101 brings the batches
back.*

Raising the limit has its own price. A 150-column projection let into one stage with
`maxFields=200` ran two and a half to three and a half times slower for its first 12 to 22
seconds, while C2 compiled the bigger method.

## In the source: 1024 characters and 255 slots

Outside a stage, Spark splits generated code into methods by counting characters, every 1024 of
them (`spark.sql.codegen.methodSplitThreshold`). It counts characters because, in the setting's
own words, "We cannot know how many bytecode will be generated." On a projection of forty
`x + k` it comes to about seven characters per byte of bytecode, so the default makes methods of
235 bytes. Inside a stage this split doesn't run, which is how a stage's one method can grow past
8000 bytes.

The JVM gives a method at most 255 parameter slots. A split-out function needs one for every
value it reads, two for a `long` or a `double`, and one more for each nullable value's null flag.
When that would pass 255, Spark keeps the code in one method and says so at INFO.

## At Janino: 65,535 bytes

A Java method can hold at most 65,535 bytes of code, and a class at most 65,535 constant pool
entries. Past either, the class doesn't compile at all. Spark has two fallbacks. A stage runs its
operators one by one instead, an eighth to a fifth slower on the shape we measured. A projection
outside a stage falls back to the interpreter, 3.1 to 4.4 times the compiled time. About thirty
places call a code generator with no fallback, and there the query fails.
`explain("codegen")` prints the constant pool's size and its share used.

Spark also has a check of its own here. After Janino compiles a stage, Spark reads every method's
size and compares it with `spark.sql.codegen.hugeMethodLimit`. That setting defaults to 65535,
a size no method that compiled can reach, so by default the check never fires.

## In the cache: 100 classes

Every compiled class goes into a per-JVM cache of 100 (`spark.sql.codegen.cache.maxEntries`,
fixed when the JVM starts), keyed by the generated source text. A hit skips Janino. A miss
compiles and logs one `Code generated in ... ms` line at INFO. Because some literals are written
into the source, a query that differs only in a number or a date is a miss. [Link to row 258's
post, which measures what that costs, when it is published. What happens past the 100 entries
is owed: its measurement 5 runs 150 distinct queries twice and counts the second loop's
compiles.]

## At the JIT: two limits of 8000

The first is HotSpot's `HugeMethodLimit`. A method over 8000 bytes of bytecode is never JIT
compiled. It runs in the bytecode interpreter for as long as the query runs.

![The step, on stock Spark 4.2.0 and three JDKs](figures/svg/fig12-the-step-on-three-jdks.svg)

*Figure 3. A projection of `greatest(add_months(d, k), date_add(d, k), last_day(d))` crosses
8000 bytes between 48 and 52 columns, and the time per row steps up there on every JDK.*

On stock 4.2.0 with JDK 25 that step is from about 1,500 to about 9,200 nanoseconds per row:
6.2 times. Spark logs the method at INFO (a warning from 4.4.0) and runs it. Setting
`spark.sql.codegen.hugeMethodLimit=8000` makes Spark fall back instead, which turns the step into
a slope and changes nothing below the limit.

The second is C2's `DesiredMethodLimit`, its budget for how much bytecode it inlines into one
compilation. It isn't a size a method has to stay under; callers spend it. A stage that writes
fifty cheap columns is a method of only 1,861 bytes, well under the first limit, and C2 still
declines to inline its fifty callees, so every row pays fifty calls. On three machines that made
the stage 1.16 to 1.41 times slower than running the same projection outside one. No product JDK
lets you change the budget. Lowering `maxFields` below the projection's width takes it out of the
stage instead.

## While it runs: 16 bits

A generated aggregate whose keys and buffers the fast hash map supports probes that map first,
and a key it can't place falls through to the regular hash map, in the same generated code. The
default capacity is 16 bits (`spark.sql.codegen.aggregate.fastHashMap.capacityBit`, 10 to 30),
room for 2^16 entries. A key type the map doesn't support, such as a struct, gets no fast map at
all, and that is the one case Spark logs, at INFO. [What it costs to pass the capacity is not
measured; it is owed, and until then this section says only what the source does.]

## The whole list

| number | what it bounds | past it | how you see it | what moves it |
|:--|:--|:--|:--|:--|
| 100 | fields an operator may have and stay in whole-stage codegen; fields of a cached table read in batches | the operator runs outside the stage; an all-numeric cached table is read row by row | no `*(n)` before the operator in `EXPLAIN` | `spark.sql.codegen.maxFields` (since 2.0.0) |
| 1024 | characters of source per split method, outside a stage | the code is cut into another method | the split methods in `explain("codegen")` | `spark.sql.codegen.methodSplitThreshold` (3.0.0) |
| 255 | parameter slots of a JVM method | Spark keeps the code in one method instead of splitting it | an INFO line naming 255 | nothing; the JVM fixes it |
| 65,535 | bytes of code in a method, and entries in a class's constant pool | the class does not compile | a WARN line, or a failed query | smaller stages |
| 65535 | `spark.sql.codegen.hugeMethodLimit`'s default | nothing: no method that compiled is that large | | set it to 8000 |
| 100 | generated classes in the per-JVM cache | the least used are evicted and compiled again when needed | one `Code generated in ... ms` INFO line per compile | `spark.sql.codegen.cache.maxEntries` (static, 2.4.0) |
| 8000 | bytes of bytecode HotSpot will JIT compile in one method | the method runs in the bytecode interpreter | `maxMethodCodeSize` in `explain("codegen")`; an INFO line, a warning from 4.4.0 | `spark.sql.codegen.hugeMethodLimit=8000` makes Spark fall back instead |
| 8000 | bytes C2 inlines into one compilation | callees stay real calls | `failed to inline: size > DesiredMethodLimit` under `-XX:+PrintInlining` | nothing on a product JDK |
| 16 | the bits of the generated fast hash map's capacity | new keys go to the regular hash map | not reported | `spark.sql.codegen.aggregate.fastHashMap.capacityBit` (2.4.0, 10 to 30) |

## How these were checked

Every default was read from Spark 4.2.0's source at its release tag (`SQLConf`,
`StaticSQLConf`, `CodeGenerator.scala` and `HashAggregateExec.scala` at `v4.2.0`). Every
measurement comes from one of three longer posts, each naming its committed results file, its
machine and its release: [The 8000-byte cliff in Spark
SQL](https://vecbricks.github.io/the-8000-byte-cliff/), [When Spark stops compiling your
query](https://vecbricks.github.io/when-spark-stops-compiling-your-query/) and [Under 8000 bytes
by construction](https://vecbricks.github.io/under-8000-bytes-by-construction/). The code and the
results are in [vecbricks/varka](https://github.com/vecbricks/varka), where we also build a code
generator that reads every method's size off the class before it runs.
