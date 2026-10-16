# Spark's code generator by the numbers

*Draft for row 260, 16 October 2026. Every default is read from Spark 4.2.0's source (`SQLConf`,
`StaticSQLConf`, `CodeGenerator.scala` and `HashAggregateExec.scala` at tag `v4.2.0`); every
measurement is quoted from a published post that names its committed results file, its machine and
its release. Two rows have no measurement yet; see `VARKA-260.md` 9.1.*

Spark SQL compiles each stage of a query into one generated Java method, and a handful of numbers
decide what goes into that method and how well the JVM runs it. They are spread over Spark's
settings, its code generator and HotSpot, and most of them are marked internal, so they do not
appear in the configuration page. This is the list on one page: the number, what happens past it,
how you would see it, and what moves it. It is a reference for the three longer posts that
measured them:
[The 8000-byte cliff in Spark SQL](https://vecbricks.github.io/the-8000-byte-cliff/),
[When Spark stops compiling your
query](https://vecbricks.github.io/when-spark-stops-compiling-your-query/) and
[Under 8000 bytes by construction](https://vecbricks.github.io/under-8000-bytes-by-construction/).

| number | what it bounds | past it | how you see it | what moves it |
|:--|:--|:--|:--|:--|
| 100 | fields an operator may have and stay in whole-stage codegen; fields of a cached table read in batches | the operator runs outside the stage; an all-numeric cached table is read row by row | no `*(n)` before the operator in `EXPLAIN` | `spark.sql.codegen.maxFields` (since 2.0.0) |
| 1024 | characters of source per split method, outside a stage | the code is cut into another method | the split methods in `explain("codegen")` | `spark.sql.codegen.methodSplitThreshold` (3.0.0) |
| 255 | parameter slots of a JVM method | Spark keeps the code in one method instead of splitting it | an INFO line naming 255 | nothing; the JVM fixes it |
| 8000 | bytes of bytecode HotSpot will JIT compile in one method | the method runs in the bytecode interpreter | `maxMethodCodeSize` in `explain("codegen")`; an INFO line, a warning from 4.4.0 | `spark.sql.codegen.hugeMethodLimit=8000` makes Spark fall back instead |
| 8000 | bytes C2 inlines into one compilation | callees stay real calls | `failed to inline: size > DesiredMethodLimit` under `-XX:+PrintInlining` | nothing on a product JDK |
| 65535 | `spark.sql.codegen.hugeMethodLimit`'s default | nothing: no method that compiled is that large | | set it to 8000 |
| 65,535 | bytes of code in a method, and entries in a class's constant pool | the class does not compile | a WARN line, or a failed query | smaller stages |
| 100 | generated classes in the per-JVM cache | the least used are evicted and compiled again when needed | one `Code generated in ... ms` INFO line per compile | `spark.sql.codegen.cache.maxEntries` (static, 2.4.0) |
| 16 | the bits of the generated fast hash map's capacity | new keys go to the regular hash map | not reported | `spark.sql.codegen.aggregate.fastHashMap.capacityBit` (2.4.0, 10 to 30) |

## The two limits of 8000

The first is HotSpot's `HugeMethodLimit`. A method past 8000 bytes of bytecode is never JIT
compiled; it runs in the interpreter for as long as the query runs. A projection of
`greatest(add_months(d, k), date_add(d, k), last_day(d))` crosses it between 48 and 52 columns, and
on stock 4.2.0 with JDK 25 the time per row goes from about 1,500 to about 9,200 nanoseconds there,
6.2 times. Spark measures every compiled stage against 8000 and acts on it only when
`spark.sql.codegen.hugeMethodLimit` is 8000 too; its default has been 65535 since before 2.3.0
shipped, a size no compiled method reaches, so by default the stage is logged at INFO and run.
With the setting at 8000 the step becomes a slope, and below the limit it changes nothing.

The second is C2's `DesiredMethodLimit`, the budget for how much bytecode it inlines into one
compilation. It is not a size a method must stay under; a caller spends it. A stage that writes
fifty cheap columns is a method of 1,861 bytes, well under the first limit, and C2 still refuses
fifty inlines, so each row pays fifty calls: 1.16 to 1.41 times slower in the stage than outside
it on three machines. No product JDK lets you change it; lowering `maxFields` below such a
projection's width takes it out of the stage.

## 100 fields, both ways

`spark.sql.codegen.maxFields` decides two things. An operator wider than it leaves whole-stage
codegen, silently. And a cached table is read in batches only if every column is a primitive number
or boolean and the table is within it, counting the whole table, not the columns read. Reading
every column of a 101-column cached table cost five times the 100-column table on one runner and
two and a half times on a Xeon. Raising the limit fixes that and has a price: a 150-column
projection let into one stage by `maxFields=200` ran two and a half to three and a half times
slower for its first 12 to 22 seconds, while C2 compiled it.

## 1024 characters

Spark splits generated code into methods by counting characters, because, in the setting's own
words, "We cannot know how many bytecode will be generated." On a projection of forty `x + k` it
comes to about seven characters a byte, so the default makes methods of 235 bytes. Inside a stage
this split of expressions does not run, which is why a stage's method can grow past 8000 bytes.

## 255 and 65,535

These are the JVM's. A split function needs a parameter slot for every value it reads, two for a
`long` or a `double` and one more for a nullable value's null flag; when that would pass 255,
Spark keeps the code in one method and says so at INFO. A method past 65,535 bytes of code does
not compile, and Spark has two fallbacks: a stage runs its operators one by one, an eighth to a
fifth slower on the shape measured, and a projection outside a stage falls back to the
interpreter, 3.1 to 4.4 times the compiled time. About thirty places call a code generator with no
fallback, and there the query fails. The constant pool fails the same way at 65,535 entries;
`explain("codegen")` prints the pool's size and its share used.

## 100 classes

Every compiled class goes into a cache of 100, per JVM, keyed by the generated source. A hit skips
Janino; a miss compiles, and logs one `Code generated in ... ms` line at INFO. Because some literals
are written into the source, a query that differs only in a number or a date misses it; How Spark
compiles a query, and why it compiles it again [link to row 258's post when published] measures what
that costs. The cache is static: it can only be set when the JVM starts. [What happens past the
100 entries is owed: row 258's measurement 5 runs 150 distinct queries twice and counts the second
loop's compiles.]

## 16 bits

An aggregate whose keys and buffers the generated fast hash map supports probes it first, and a key
it cannot place there falls through to the regular hash map, in the same generated code. The
default of 16 bits sizes it for 2^16 rows. A key type the map does not support, such as a struct,
gets no fast map at all, and that is the one case Spark logs, at INFO. [What it costs to pass the
capacity is not measured; it is listed as owed and the row stays as the source describes it.]

## How these were checked

Each default above was read from Spark 4.2.0's source at its release tag, and each measurement
comes from one of the three posts linked at the top, which name their runs, machines and results
files. The code and the results are in [vecbricks/varka](https://github.com/vecbricks/varka),
where we also build a code generator that reads every method's size off the class before it runs.
