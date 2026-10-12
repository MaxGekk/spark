/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions.codegen.varka;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.time.temporal.IsoFields;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.IntToLongFunction;
import java.util.function.IntUnaryOperator;

import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.AddDays;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ColumnRef;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LastDay;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LiteralSlot;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.MakeDate;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.SubDays;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.TruncDate;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.TruncLevel;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.DateTimeUtils$;

/**
 * The fixtures every emitter suite shares (VARKA-268): the emitted class built and loaded, the
 * input and output columns over Arrow-shaped memory, the data the suites drive, and three sweeps
 * of every day of the narrow range against {@code DateTimeUtils} and {@code LocalDate}.
 * {@code VarkaEmitterTestBase} is the Scala layer the suites mix in over it; the two matrices
 * that drive the Scala reference evaluator stay there. {@link VarkaEmitterTestSupport} beside it
 * reads emitted class files back.
 */
public final class VarkaEmitterFixtures {

  private VarkaEmitterFixtures() {}

  private static final AtomicInteger CLASS_COUNTER = new AtomicInteger(0);

  // ---------------------------------------------------------------------------------------------
  // The emitted class
  // ---------------------------------------------------------------------------------------------

  /** {@code date_add(c0, lit[offsetSlot])}. */
  public static VarkaVectorIR addDays(int offsetSlot) {
    return new AddDays(new ColumnRef(0), new LiteralSlot(offsetSlot));
  }

  /** An {@code AddDays}/{@code SubDays} chain of the given depth, alternating so C2 cannot
   * reassociate it. */
  public static VarkaVectorIR chain(int depth, int slotBase) {
    VarkaVectorIR node = new ColumnRef(0);
    for (int level = 0; level < depth; level++) {
      node = level % 2 == 0 ? new AddDays(node, new LiteralSlot(slotBase + level))
          : new SubDays(node, new LiteralSlot(slotBase + level));
    }
    return node;
  }

  /** An emitted class: its name and its bytes. */
  public record Emitted(String className, byte[] bytes) {}

  /** A loaded kernel and the per-task loader it came from, which the caller releases. */
  public record Loaded(VarkaFusedKernel kernel, VarkaGeneratedClassLoader loader) {}

  /**
   * Emits {@code roots} into a uniquely named class. The emitter's non-shape inputs travel as a
   * {@link VarkaEmitOptions} value on the call (VARKA-23), so a variant is a different argument.
   */
  public static Emitted emit(List<VarkaVectorIR> roots, int numInputs, int numLiterals,
      VarkaEmitOptions options) {
    String name = "org.apache.spark.sql.varka.execution.VarkaFusedTest"
        + CLASS_COUNTER.addAndGet(1);
    return new Emitted(name,
        VarkaLoopEmitter.emit(name, roots, numInputs, numLiterals, null, null, options));
  }

  /** Loads an emitted class through the per-task loader and instantiates it. */
  public static Loaded load(Emitted emitted) {
    VarkaSpeciesGuard.check(emitted.className(), emitted.bytes());
    var loader = new VarkaGeneratedClassLoader(VarkaEmitterFixtures.class.getClassLoader());
    loader.defineGeneratedClass(emitted.className(), emitted.bytes());
    try {
      var kernel = (VarkaFusedKernel) loader.loadClass(emitted.className())
          .getConstructor().newInstance();
      return new Loaded(kernel, loader);
    } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException
        | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException("cannot instantiate " + emitted.className(), e);
    }
  }

  /** The declared method names of an emitted class - how the method layout is asserted. */
  public static List<String> methodNames(Emitted emitted) {
    var loader = new VarkaGeneratedClassLoader(VarkaEmitterFixtures.class.getClassLoader());
    loader.defineGeneratedClass(emitted.className(), emitted.bytes());
    try {
      var names = new ArrayList<String>();
      for (Method m : loader.loadClass(emitted.className()).getDeclaredMethods()) {
        names.add(m.getName());
      }
      return names;
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("cannot load " + emitted.className(), e);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Columns
  // ---------------------------------------------------------------------------------------------

  /** One column's worth of buffers: data, validity bitmap and its null count. */
  public record Col(MemorySegment data, MemorySegment validity, int nullCount) {
    /** Per the kernel contract a null-free or all-null column may pass 0L for its validity. */
    public long validityAddress(int length) {
      return nullCount == 0 || nullCount == length ? 0L : validity.address();
    }
  }

  /** An output column: data, and the validity bitmap the kernel writes. */
  public record Output(MemorySegment data, MemorySegment validity) {}

  public static MemorySegment alloc(Arena arena, long bytes) {
    return arena.allocate(Math.max(bytes, 1L), 8);
  }

  /**
   * The value a null slot holds: poisoned, not zeroed (VARKA-70's harness change, made before
   * that task's emitter work so the whole existing matrix runs against it first). Arrow leaves the
   * data under a null slot undefined, the loop body loads every column unmasked, and the only
   * things standing between a null lane's garbage and a wrong answer are the validity word - which
   * the range guards AND with their condemning mask (VARKA-42, VARKA-52, VARKA-60) - and the rule
   * that no lowering traps on any int. Until this landed the harness wrote the caller's own value
   * into a null slot - the same in-range day or count the valid rows carry - which cannot tell a
   * kernel that honours the word from one that never had to: an in-range lane reaches no guard's
   * condemning comparison whether it is masked or not. Alternating the two extremes can, and puts
   * each on both sides of every guard's bound.
   *
   * <p>The alternation counts null slots, not row indices. Keying it on {@code i} would collide
   * with the null patterns, which are themselves index predicates: under {@code alternating}
   * ({@code i % 2 == 1}) every null row is odd, so an {@code i & 1} poison would write
   * {@code Integer.MAX_VALUE} in every one of them and a quarter of the matrix would only ever see
   * the upper side of a bound. The bounds are asymmetric - {@code MAKE_DATE_MIN_YEAR} against
   * {@code MAKE_DATE_MAX_YEAR}, and VARKA-69 widens {@code dayRange} in one direction only - so a
   * mask applied to the {@code > MAX} comparison but not the {@code < MIN} one would stay green.
   * Counting null slots alternates whatever the pattern is.
   */
  public static int poison(int nullOrdinal) {
    return (nullOrdinal & 1) == 0 ? Integer.MIN_VALUE : Integer.MAX_VALUE;
  }

  /** {@link #makeInputData} over the harness's default values, nulls poisoned. */
  public static Col makeInput(Arena arena, int length, IntPredicate isNull) {
    return makeInputData(arena, length, isNull, i -> i * 31 - 7000, true);
  }

  /**
   * An int column of {@code length} rows, {@code isNull} choosing the null rows.
   *
   * @param poisonNulls false where the caller has deliberately placed a value at a row it also
   *   marks null - the guard tests that pin a boundary value in a null lane and assert the batch
   *   is not condemned. Poisoning those would substitute an extreme for the boundary the test
   *   names, and one of them ({@code MAKE_DATE_MIN_YEAR - 1} under a null year) would silently
   *   become a duplicate of the case above it. Every other caller leaves it on; the pattern
   *   matrix, which is where a lowering that forgets to AND the guard with the word gets caught,
   *   is entirely on the poisoned path.
   */
  public static Col makeInputData(Arena arena, int length, IntPredicate isNull,
      IntUnaryOperator value, boolean poisonNulls) {
    MemorySegment data = alloc(arena, length * 4L);
    MemorySegment validity = alloc(arena, (length + 7) / 8L);
    validity.fill((byte) 0);
    int nulls = 0;
    for (int i = 0; i < length; i++) {
      if (isNull.test(i)) {
        data.set(ValueLayout.JAVA_INT, i * 4L, poisonNulls ? poison(nulls) : value.applyAsInt(i));
        nulls++;
      } else {
        data.set(ValueLayout.JAVA_INT, i * 4L, value.applyAsInt(i));
        long off = i / 8L;
        byte old = validity.get(ValueLayout.JAVA_BYTE, off);
        validity.set(ValueLayout.JAVA_BYTE, off, (byte) (old | (1 << (i % 8))));
      }
    }
    return new Col(data, validity, nulls);
  }

  /**
   * An int output column. The data holds a sentinel no chain produces from the inputs, so an
   * unwritten valid row shows. The validity is the nominal size unless {@code validityBytes} is
   * not negative, and a bounded segment is a real assertion: a per-group write addressing more
   * than the bytes its group occupies faults here rather than corrupting a neighbour. VARKA-47's
   * word writer needs whole words instead, which is what an Arrow destination buffer actually
   * carries (VarkaKernelEvaluatorSuite), so its arm asks for that size explicitly rather than
   * widening every other test's guard. It starts all ones: the loop must zero it, and stale bits
   * must not leak through.
   */
  public static Output makeOutput(Arena arena, int length, long validityBytes) {
    MemorySegment data = alloc(arena, length * 4L);
    for (int i = 0; i < length; i++) {
      data.set(ValueLayout.JAVA_INT, i * 4L, 0xDEADBEEF);
    }
    MemorySegment validity =
        alloc(arena, validityBytes >= 0 ? validityBytes : (length + 7) / 8L);
    validity.fill((byte) 0xFF);
    return new Output(data, validity);
  }

  /** The destination validity bytes an emission under {@code options} needs. */
  public static long outputValidityBytes(VarkaEmitOptions options, int length) {
    return options.validityByWord() ? ((length + 63L) / 64L) * 8L : (length + 7L) / 8L;
  }

  /** Asserts two outputs agree bit for bit and, where valid, value for value. */
  public static void assertSameOutput(int length, Output expected, Output actual,
      String context) {
    for (long b = 0; b < (length + 7) / 8L; b++) {
      byte want = expected.validity().get(ValueLayout.JAVA_BYTE, b);
      byte got = actual.validity().get(ValueLayout.JAVA_BYTE, b);
      if (got != want) {
        throw new IllegalStateException(got + " did not equal " + want + " " + context
            + ": validity byte " + b + " differs");
      }
    }
    for (int i = 0; i < length; i++) {
      boolean valid =
          (expected.validity().get(ValueLayout.JAVA_BYTE, i / 8L) & (1 << (i % 8))) != 0;
      if (valid) {
        int want = expected.data().get(ValueLayout.JAVA_INT, i * 4L);
        int got = actual.data().get(ValueLayout.JAVA_INT, i * 4L);
        if (got != want) {
          throw new IllegalStateException(got + " did not equal " + want + " " + context
              + ": row " + i + " differs");
        }
      }
    }
  }

  /**
   * One 64-bit column: eight bytes a lane, and the same validity bitmap the int lane uses. A null
   * lane holds a poison value, as the int harness does, so a kernel that reads one gets a number
   * no case expects; a test whose subject is the value under a null lane passes
   * {@code poisonNulls = false}, because poison would replace what it is measuring.
   */
  public static Col makeLongInput(Arena arena, int length, IntPredicate isNull,
      IntToLongFunction value, boolean poisonNulls) {
    MemorySegment data = alloc(arena, length * 8L);
    MemorySegment validity = alloc(arena, (length + 7) / 8L);
    validity.fill((byte) 0);
    int nulls = 0;
    for (int i = 0; i < length; i++) {
      if (isNull.test(i)) {
        data.set(ValueLayout.JAVA_LONG, i * 8L,
            poisonNulls ? Long.MIN_VALUE + 7L + nulls : value.applyAsLong(i));
        nulls++;
      } else {
        data.set(ValueLayout.JAVA_LONG, i * 8L, value.applyAsLong(i));
        validity.set(ValueLayout.JAVA_BYTE, i / 8L,
            (byte) (validity.get(ValueLayout.JAVA_BYTE, i / 8L) | (1 << (i % 8))));
      }
    }
    return new Col(data, validity, nulls);
  }

  /**
   * {@link #makeOutput}'s twin at 64-bit lanes, with the same two assertions: data poisoned with
   * a sentinel no case computes, and validity bytes that start all ones.
   */
  public static Output makeLongOutput(Arena arena, int length) {
    MemorySegment data = alloc(arena, length * 8L);
    for (int i = 0; i < length; i++) {
      data.set(ValueLayout.JAVA_LONG, i * 8L, 0xDEADBEEFCAFEBABEL);
    }
    MemorySegment validity = alloc(arena, (length + 7) / 8L);
    validity.fill((byte) 0xFF);
    return new Output(data, validity);
  }

  /** Runs a two-input kernel with one output, returning the batch status. */
  public static int runKernel2(VarkaFusedKernel kernel, Col a, Col b, Output out, int length) {
    return kernel.run(
        new long[] {a.data().address(), b.data().address()},
        new long[] {a.validityAddress(length), b.validityAddress(length)},
        new int[] {a.nullCount(), b.nullCount()},
        new long[] {out.data().address()}, new long[] {out.validity().address()}, new int[0],
        length, VarkaEmitterTestSupport.scratch(kernel, length));
  }

  // ---------------------------------------------------------------------------------------------
  // The data the suites drive
  // ---------------------------------------------------------------------------------------------

  /**
   * Boundary-straddling lengths for every species this can run at (4, 8 or 16 lanes), plus the
   * byte boundaries of the bit-packed validity, plus batch-sized ones.
   */
  public static final int[] LENGTHS =
      {0, 1, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 1000, 4096, 4097};

  public static final int[] OFFSETS = {0, 1, -1, 3, Integer.MAX_VALUE - 1};

  /**
   * Lengths deliberately chosen odd or prime: a lane count divides 64 and 1000 but none of these,
   * so every case leaves a remainder and so exercises the epilogue - which under today's grouping
   * is the only body that holds two calendar outputs at once, and therefore the only body where
   * sharing does anything at all.
   */
  public static final int[] REMAINDER_LENGTHS = {1, 13, 17, 63, 1001};

  public static int defaultData(int col, int i) {
    return (i * (col + 3)) % 23 - 11;
  }

  private static int epochDay(int year, int month, int day) {
    return (int) LocalDate.of(year, month, day).toEpochDay();
  }

  /** The calendar boundary set every bounded calendar test walks. */
  public static final int[] CALENDAR_BOUNDARY_DAYS = {
      VarkaChrono.NARROW_MIN_DAYS, VarkaChrono.NARROW_MIN_DAYS + 1,
      VarkaChrono.NARROW_MAX_DAYS, VarkaChrono.NARROW_MAX_DAYS - 1,
      -1, 0, 1, -719468, -719162,
      epochDay(1600, 2, 29), epochDay(1900, 3, 1), epochDay(2000, 2, 29), epochDay(1, 1, 1),
      epochDay(9999, 12, 31),
      // dayofyear's own boundary set (VARKA-34): every year-end/year-start pair a leap flag
      // could get wrong, plus February's own boundary in a leap and a century-non-leap year.
      epochDay(2000, 1, 1), epochDay(2000, 12, 31), // leap
      epochDay(2024, 1, 1), epochDay(2024, 12, 31), // leap
      epochDay(2023, 1, 1), epochDay(2023, 12, 31), // common
      epochDay(1900, 1, 1), epochDay(1900, 12, 31), // century, not leap
      epochDay(2000, 2, 28), epochDay(1900, 2, 28)
  };

  public static int calendarBoundaryDay(int c, int i) {
    return i < CALENDAR_BOUNDARY_DAYS.length ? CALENDAR_BOUNDARY_DAYS[i] : i * 9973 - 400000;
  }

  /**
   * VARKA-37's days: the ISO corners its plan names - the week-53 years, the January days that
   * belong to the old year and the December days that belong to the new one - and Velox's
   * Spark-compatibility fixtures (velox/functions/sparksql/tests/DateTimeFunctionsTest.cpp),
   * written against Spark by people who had to match it exactly, over the calendar boundary set.
   */
  public static final int[] ISO_WEEK_DAYS = concat(new int[] {
      epochDay(2015, 12, 28), epochDay(2016, 1, 1), epochDay(2019, 12, 30),
      epochDay(2020, 12, 31), epochDay(2021, 1, 1),
      epochDay(2004, 12, 31), epochDay(2009, 12, 31), epochDay(2015, 12, 31),
      epochDay(2026, 12, 31),
      epochDay(1919, 12, 31), epochDay(1969, 12, 31), epochDay(1960, 1, 1),
      epochDay(1, 1, 1), epochDay(9999, 12, 31),
      // leap years ending on a Thursday, a Friday and a Saturday
      epochDay(2020, 12, 31), epochDay(2004, 12, 31), epochDay(2016, 12, 31)
  }, CALENDAR_BOUNDARY_DAYS);

  public static int isoWeekDay(int c, int i) {
    return i < ISO_WEEK_DAYS.length ? ISO_WEEK_DAYS[i] : i * 9973 - 400000;
  }

  /** The days the calendar differentials drive: the range's edges, then a strided walk. */
  private static final int[] CALENDAR_DAY_EDGES = {
      VarkaChrono.NARROW_MIN_DAYS, VarkaChrono.NARROW_MIN_DAYS + 1,
      VarkaChrono.NARROW_MAX_DAYS, VarkaChrono.NARROW_MAX_DAYS - 1,
      -1, 0, 1, -719468,
      epochDay(1600, 2, 29), epochDay(1900, 3, 1), epochDay(2000, 2, 29), epochDay(2023, 12, 31)
  };

  public static int calendarDays(int c, int i) {
    return i < CALENDAR_DAY_EDGES.length ? CALENDAR_DAY_EDGES[i] : i * 9973 - 400000;
  }

  /** VARKA-35: trunc(date, YEAR | MONTH | QUARTER), two lowerings behind the options. */
  public static final List<VarkaVectorIR> TRUNC_ROOTS = List.of(
      new TruncDate(new ColumnRef(0), TruncLevel.YEAR),
      new TruncDate(new ColumnRef(0), TruncLevel.MONTH),
      new TruncDate(new ColumnRef(0), TruncLevel.QUARTER));

  /** A {@code make_date} argument triple. */
  public record Triple(int year, int month, int day) {}

  /**
   * VARKA-42: make_date over three int columns. The triples cover the validity rule's corners:
   * valid dates at both ends of the contract, 29 February in leap, common, century and
   * quatercentennial years, 30 February, 31 April, 32 December, month 0, 13 and -1, day 0 and -1.
   */
  public static final Triple[] MAKE_DATE_ALL = {
      new Triple(2024, 1, 1), new Triple(2024, 2, 29), new Triple(2023, 2, 29),
      new Triple(1900, 2, 29), new Triple(2000, 2, 29), new Triple(2024, 2, 30),
      new Triple(2024, 4, 31), new Triple(2024, 4, 30), new Triple(2024, 12, 31),
      new Triple(2024, 12, 32), new Triple(2024, 13, 1), new Triple(2024, 0, 1),
      new Triple(2024, -1, 15), new Triple(2024, 1, 0), new Triple(2024, 6, -1),
      new Triple(1, 1, 1), new Triple(9999, 12, 31), new Triple(1970, 1, 1),
      new Triple(1969, 12, 31), new Triple(VarkaChrono.MAKE_DATE_MIN_YEAR, 1, 1),
      new Triple(VarkaChrono.MAKE_DATE_MAX_YEAR, 12, 31)
  };

  /** The triples {@code make_date} answers with a date. */
  public static final Triple[] MAKE_DATE_VALID = Arrays.stream(MAKE_DATE_ALL)
      .filter(t -> VarkaChrono.makeDate(t.year(), t.month(), t.day())
          >= VarkaChrono.MAKE_DATE_OUT_OF_RANGE + 1)
      .toArray(Triple[]::new);

  /** Column {@code c} of the {@code i}-th triple, cycling: 0 the year, 1 the month, 2 the day. */
  public static int tripleData(Triple[] triples, int c, int i) {
    Triple t = triples[i % triples.length];
    return c == 0 ? t.year() : c == 1 ? t.month() : t.day();
  }

  public static final MakeDate MAKE_DATE_NULL =
      new MakeDate(new ColumnRef(0), new ColumnRef(1), new ColumnRef(2), false);

  private static final int[] EXTREMES = {Integer.MAX_VALUE, Integer.MIN_VALUE,
      Integer.MAX_VALUE - 1, Integer.MIN_VALUE + 1, 0, 1, -1, 100, -100, 7,
      Integer.MAX_VALUE / 2, Integer.MIN_VALUE / 2};

  /** Values that put the sign test under load: both extremes, their neighbours, and zero. */
  public static int extreme(int col, int i) {
    return EXTREMES[(i + col * 5) % EXTREMES.length];
  }

  private static final int[] SMALLS = {0, 1, -1, 7, -7, 100, -100, 30000, -30000, 46340, -46340};

  /** The same spread, kept small enough that no op over two of them can overflow. */
  public static int small(int col, int i) {
    return SMALLS[(i + col * 3) % SMALLS.length];
  }

  // ---------------------------------------------------------------------------------------------
  // What an emitted class costs
  // ---------------------------------------------------------------------------------------------

  /**
   * The lane ops one emitted body method runs: its {@code IntVector} invocations, counted off the
   * class file. VARKA-48's deliverable is a count, not a duration, so it is asserted as one.
   */
  public static int laneOps(byte[] bytes, String method) {
    return VarkaEmitterTestSupport.invocationCount(bytes, method,
        "jdk.incubator.vector.IntVector");
  }

  /**
   * What the prefix's month step costs, which depends on the axis: four ops on the 0-based one
   * (the {@code * 5}, the {@code + 2}, the magic multiply and its shift) and two on VARKA-53's
   * 3-based one (the {@code * 2141} and the {@code + 197913}). VARKA-48's elision saves whichever
   * of the two the shape was going to pay, which is the sense in which that task's win shrank
   * rather than went away.
   */
  public static int monthStepOps(VarkaEmitOptions options) {
    return options.neriSchneiderMonth() ? 2 : 4;
  }

  /**
   * The single masked epilogue's bytecode size in the form before VARKA-87 - the one method every
   * output shared (VARKA-24) - whatever {@code options} says about the byte budget. The tests that
   * pin where that method crossed HugeMethodLimit measure this form on purpose: the crossing is
   * the fact the per-group epilogue answers, and it stays measurable at budget 0.
   */
  public static int singleEpilogueSize(List<VarkaVectorIR> roots, int numInputs,
      VarkaEmitOptions options) {
    return VarkaEmitterTestSupport.codeSize(
        emit(roots, numInputs, 0, options.withMethodByteBudget(0)).bytes(), "epilogueMasked");
  }

  // ---------------------------------------------------------------------------------------------
  // The sweeps: every day of the narrow range
  // ---------------------------------------------------------------------------------------------

  /** What {@code LocalDate} says the five extractions are, in the order they are emitted. */
  public static final Function<LocalDate, int[]> ALL_CALENDAR_FIELDS = date -> new int[] {
      date.getYear(), date.getMonthValue(), date.getDayOfMonth(),
      date.get(IsoFields.QUARTER_OF_YEAR), date.getDayOfYear()};

  /** The rows of one sweep chunk. */
  private static final int CHUNK = 1 << 16;

  /**
   * One of the three sweeps: the kernel for {@code roots} over every day of the narrow range, a
   * chunk at a time, each chunk's outputs handed to {@code check} with the chunk's first day and
   * row count. A declined chunk fails.
   */
  private static void sweep(List<VarkaVectorIR> roots, VarkaEmitOptions options,
      ChunkCheck check) {
    Loaded loaded = load(emit(roots, 1, 0, options));
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment data = alloc(arena, CHUNK * 4L);
      MemorySegment validity = alloc(arena, (CHUNK + 7) / 8L);
      validity.fill((byte) 0xFF);
      var outs = new Output[roots.size()];
      var outData = new long[roots.size()];
      var outValidity = new long[roots.size()];
      for (int o = 0; o < outs.length; o++) {
        outs[o] = makeOutput(arena, CHUNK, -1L);
        outData[o] = outs[o].data().address();
        outValidity[o] = outs[o].validity().address();
      }
      int day = VarkaChrono.NARROW_MIN_DAYS;
      while (day <= VarkaChrono.NARROW_MAX_DAYS) {
        int n = Math.min(CHUNK, VarkaChrono.NARROW_MAX_DAYS - day + 1);
        for (int i = 0; i < n; i++) {
          data.set(ValueLayout.JAVA_INT, i * 4L, day + i);
        }
        int status = loaded.kernel().run(new long[] {data.address()},
            new long[] {validity.address()}, new int[] {0}, outData, outValidity, new int[0], n,
            VarkaEmitterTestSupport.scratch(loaded.kernel(), n));
        if (status != 0) {
          throw new IllegalStateException(status + " did not equal 0 the kernel declined an "
              + "in-range batch at day " + day);
        }
        check.check(outs, day, n);
        day += n;
      }
    } finally {
      loaded.loader().release();
    }
  }

  /** The comparison one sweep makes over a chunk's outputs; throws on the first difference. */
  private interface ChunkCheck {
    void check(Output[] outs, int firstDay, int rows);
  }

  /** The emitted trunc kernels against {@code DateTimeUtils.truncDate}, every day, all levels. */
  public static void sweepTrunc(VarkaEmitOptions options) {
    // private[sql] Scala values, so read through the object's module, as VarkaChronoCompiler does.
    int[] levels = {DateTimeUtils$.MODULE$.TRUNC_TO_YEAR(), DateTimeUtils$.MODULE$.TRUNC_TO_MONTH(),
        DateTimeUtils$.MODULE$.TRUNC_TO_QUARTER()};
    sweep(TRUNC_ROOTS, options, (outs, first, rows) -> {
      for (int i = 0; i < rows; i++) {
        int d = first + i;
        for (int o = 0; o < levels.length; o++) {
          int got = outs[o].data().get(ValueLayout.JAVA_INT, i * 4L);
          int want = DateTimeUtils.truncDate(d, levels[o]);
          if (got != want) {
            throw new IllegalStateException("day " + d + " level " + levels[o] + " under "
                + options.canonical() + ": emitted " + got + ", DateTimeUtils.truncDate " + want);
          }
        }
      }
    });
  }

  /**
   * The emitted calendar kernel for {@code roots} against {@code expected}, which says what
   * {@code LocalDate} makes of a day, in the order of the roots.
   */
  public static void sweepCalendar(List<VarkaVectorIR> roots, VarkaEmitOptions options,
      Function<LocalDate, int[]> expected) {
    var got = new int[roots.size()];
    sweep(roots, options, (outs, first, rows) -> {
      for (int i = 0; i < rows; i++) {
        LocalDate date = LocalDate.ofEpochDay(first + i);
        for (int o = 0; o < got.length; o++) {
          got[o] = outs[o].data().get(ValueLayout.JAVA_INT, i * 4L);
        }
        int[] want = expected.apply(date);
        if (!Arrays.equals(got, want)) {
          throw new IllegalStateException("day " + (first + i) + " (" + date + "), shared="
              + options.shareChronoPrefix() + ", elided=" + options.elideChronoMonth()
              + ": emitted " + Arrays.toString(got) + ", LocalDate " + Arrays.toString(want));
        }
      }
    });
  }

  /** The emitted last_day kernel against {@code DateTimeUtils.getLastDayOfMonth}, every day. */
  public static void sweepLastDay(VarkaEmitOptions options) {
    sweep(List.of(new LastDay(new ColumnRef(0))), options, (outs, first, rows) -> {
      for (int i = 0; i < rows; i++) {
        int d = first + i;
        int got = outs[0].data().get(ValueLayout.JAVA_INT, i * 4L);
        int want = DateTimeUtils.getLastDayOfMonth(d);
        if (got != want) {
          throw new IllegalStateException("day " + d + ": emitted " + got
              + ", DateTimeUtils.getLastDayOfMonth " + want);
        }
      }
    });
  }

  private static int[] concat(int[] a, int[] b) {
    int[] out = Arrays.copyOf(a, a.length + b.length);
    System.arraycopy(b, 0, out, a.length, b.length);
    return out;
  }
}
