import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Row 396's failing kernel class, its exact bytes, called in a bare loop: the 7-row batch with
 * no nulls (the masked body with every column's null count 0), the same batch every call, every
 * row checked against the expected answer on every call. No Varka test harness is involved; the
 * class needs only Varka's runtime helpers (VarkaVectorSupport) on the class path.
 */
public class Bare396 {
  static final class Loader extends ClassLoader {
    Loader() {
      super(Bare396.class.getClassLoader());
    }

    Class<?> define(String name, byte[] b) {
      return defineClass(name, b, 0, b.length);
    }
  }

  public static void main(String[] args) throws Exception {
    byte[] bytes = Files.readAllBytes(Path.of(args[0]));
    String name = args[1];
    int calls = args.length > 2 ? Integer.parseInt(args[2]) : 200_000;
    Object kernel = new Loader().define(name, bytes).getDeclaredConstructor().newInstance();
    Method run = kernel.getClass().getMethod("run", long[].class, long[].class, int[].class,
        long[].class, long[].class, int[].class, long[].class, int.class);
    long[][] cols = {
      {4460076517051L, 0, 18898746763276L, 0, -50266100486726L, 0, -49607829850524L},
      {4421922265370L, 35258963307884L, -59385115539699L, 0, 38382192361867L, 5079289584248L, 0},
      {-36599559927214L, 0, -25562645782204L, 0, -41690266522730L, 0, 41053864906336L}};
    int n = 7;
    try (Arena arena = Arena.ofConfined()) {
      long[] src = new long[3];
      long[] valid = new long[3];
      for (int c = 0; c < 3; c++) {
        MemorySegment d = arena.allocate(8L * 16, 64);
        MemorySegment v = arena.allocate(16, 64);
        for (int i = 0; i < n; i++) {
          d.setAtIndex(ValueLayout.JAVA_LONG, i, cols[c][i]);
        }
        v.fill((byte) 0xFF);
        src[c] = d.address();
        valid[c] = v.address();
      }
      MemorySegment out = arena.allocate(8L * 16, 64);
      MemorySegment outValid = arena.allocate(16, 64);
      long[] expect = new long[n];
      boolean[] expectValid = new boolean[n];
      for (int i = 0; i < n; i++) {
        long a = cols[0][i];
        long b = cols[1][i];
        long c = cols[2][i];
        long s = b + c;
        boolean ovf = ((b ^ s) & (c ^ s)) < 0;
        expect[i] = a > b ? a : s;
        expectValid[i] = a > b || !ovf;
      }
      int[] nullCounts = {0, 0, 0};
      long wrong = 0;
      long firstWrong = -1;
      for (int call = 0; call < calls; call++) {
        run.invoke(kernel, src, valid, nullCounts, new long[] {out.address()},
            new long[] {outValid.address()}, new int[0], new long[0], n);
        for (int i = 0; i < n; i++) {
          boolean v = (outValid.get(ValueLayout.JAVA_BYTE, i >> 3) >> (i & 7) & 1) != 0;
          long got = out.getAtIndex(ValueLayout.JAVA_LONG, i);
          boolean ok = v == expectValid[i] && (!v || got == expect[i]);
          if (!ok) {
            if (wrong++ < 5) {
              System.out.printf("call %d row %d: want %s got %s%n", call, i,
                  expectValid[i] ? Long.toString(expect[i]) : "null", v ? Long.toString(got)
                  : "null");
            }
            if (firstWrong < 0) {
              firstWrong = call;
            }
          }
        }
      }
      System.out.println(wrong == 0 ? "no difference in " + calls + " calls"
          : wrong + " wrong rows, first at call " + firstWrong + " of " + calls);
      System.exit(wrong == 0 ? 0 : 1);
    }
  }
}
