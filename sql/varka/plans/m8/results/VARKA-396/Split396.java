import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * Which half of blend(fromLong(toLong(a > b))) goes wrong under AVX2: toLong alone (the long it
 * returns), fromLong alone (a mask built from a long that no mask produced), and the round trip.
 */
public class Split396 {
  static final VectorSpecies<Long> S = LongVector.SPECIES_256;

  /** toLong only: the lane bits of a > b, per group. */
  static void toLongOnly(long[] a, long[] b, long[] bits) {
    for (int i = 0; i < S.loopBound(a.length); i += S.length()) {
      bits[i / 4] = LongVector.fromArray(S, a, i)
          .compare(VectorOperators.GT, LongVector.fromArray(S, b, i)).toLong();
    }
  }

  /** fromLong only: blend by a mask built from given bits. */
  static void fromLongOnly(long[] a, long[] b, long[] bits, long[] out) {
    for (int i = 0; i < S.loopBound(a.length); i += S.length()) {
      LongVector.fromArray(S, b, i).blend(LongVector.fromArray(S, a, i),
          VectorMask.fromLong(S, bits[i / 4])).intoArray(out, i);
    }
  }

  /** The round trip, the mask stored as booleans so the mask itself is read. */
  static void roundTripMask(long[] a, long[] b, boolean[] lanes) {
    for (int i = 0; i < S.loopBound(a.length); i += S.length()) {
      long m = LongVector.fromArray(S, a, i)
          .compare(VectorOperators.GT, LongVector.fromArray(S, b, i)).toLong();
      VectorMask.fromLong(S, m).intoArray(lanes, i);
    }
  }

  public static void main(String[] args) {
    int calls = args.length > 0 ? Integer.parseInt(args[0]) : 200_000;
    long[] a = {4460076517051L, 0, 18898746763276L, 0, -50266100486726L, 7, 9, -49607829850524L};
    long[] b = {4421922265370L, 35258963307884L, -59385115539699L, 0, 38382192361867L, 5, 1, 0};
    long[] wantBits = new long[2];
    for (int i = 0; i < 8; i++) {
      if (a[i] > b[i]) {
        wantBits[i / 4] |= 1L << (i % 4);
      }
    }
    long[] bits = new long[2];
    long[] out = new long[8];
    boolean[] lanes = new boolean[8];
    boolean t = false;
    boolean f = false;
    boolean r = false;
    for (int call = 0; call < calls; call++) {
      toLongOnly(a, b, bits);
      if (!t && (bits[0] != wantBits[0] || bits[1] != wantBits[1])) {
        System.out.printf("toLong: wrong at call %d: want %x %x got %x %x%n", call, wantBits[0],
            wantBits[1], bits[0], bits[1]);
        t = true;
      }
      fromLongOnly(a, b, wantBits, out);
      for (int i = 0; i < 8 && !f; i++) {
        long want = (wantBits[i / 4] >> (i % 4) & 1) != 0 ? a[i] : b[i];
        if (out[i] != want) {
          System.out.printf("fromLong: wrong at call %d, row %d: want %d got %d%n", call, i,
              want, out[i]);
          f = true;
        }
      }
      roundTripMask(a, b, lanes);
      for (int i = 0; i < 8 && !r; i++) {
        if (lanes[i] != (a[i] > b[i])) {
          System.out.printf("round trip: wrong at call %d, lane %d: want %b got %b%n", call, i,
              a[i] > b[i], lanes[i]);
          r = true;
        }
      }
    }
    System.out.printf("toLong %s, fromLong %s, round trip %s in %d calls%n",
        t ? "WRONG" : "ok", f ? "WRONG" : "ok", r ? "WRONG" : "ok", calls);
  }
}
