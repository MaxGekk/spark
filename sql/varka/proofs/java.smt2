; Licensed to the Apache Software Foundation (ASF) under one or more
; contributor license agreements.  See the NOTICE file distributed with
; this work for additional information regarding copyright ownership.
; The ASF licenses this file to You under the Apache License, Version 2.0
; (the "License"); you may not use this file except in compliance with
; the License.  You may obtain a copy of the License at
;
;    http://www.apache.org/licenses/LICENSE-2.0
;
; Unless required by applicable law or agreed to in writing, software
; distributed under the License is distributed on an "AS IS" BASIS,
; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
; See the License for the specific language governing permissions and
; limitations under the License.

; Java's int and long arithmetic over SMT-LIB's integers: the prelude every proof in this
; directory is stated in (VARKA-240).
;
; A Java int or long is an SMT-LIB Int inside its type's range, and each operator below is
; defined the way the Java Language Specification defines it, in mathematical integers, with the
; section cited. That is the point of stating them here once: an SMT operator whose name matches
; Java's need not mean what Java means - a bit-vector shift does not mask its count, a bit-vector
; division by zero has a value - and a proof that leaned on one would prove the wrong thing.
; java_check.smt2 holds every definition to the JVM's own results over boundary operands.
;
; Why integers and not bit-vectors: a bit-vector proof of the multiply-high division times out
; under every solver tried, where the same statement in integers takes a fraction of a second,
; because each product here has a constant factor and so stays linear (VARKA-240.md 2.1). The
; definitions multiply and divide their parameters, so a proof declares QF_NIA; every proof
; applies them to constants, and what the solvers reason about is linear.
;
; dev/varka_prove.sh inserts this file after each proof's (set-logic ...) line, since SMT-LIB has
; no include. It declares no logic, asserts nothing and checks nothing.
;
; Not here: a zero divisor, which throws in Java and has no value to define - a proof that
; divides states its divisor non-zero - and the saturating D2I and D2L, which need floating-point
; theory and are row 241's.

; The ranges (JLS 4.2.1).
(define-fun jint.MIN () Int (- 2147483648))
(define-fun jint.MAX () Int 2147483647)
(define-fun jlong.MIN () Int (- 9223372036854775808))
(define-fun jlong.MAX () Int 9223372036854775807)
(define-fun jint.in ((x Int)) Bool (and (<= jint.MIN x) (<= x jint.MAX)))
(define-fun jlong.in ((x Int)) Bool (and (<= jlong.MIN x) (<= x jlong.MAX)))

; An overflowing sum or product is the low-order bits of the mathematical one in two's complement
; (JLS 15.18.2, 15.17.1): the value in range that is congruent to it modulo 2^32 or 2^64.
(define-fun jint.wrap ((v Int)) Int (- (mod (+ v 2147483648) 4294967296) 2147483648))
(define-fun jlong.wrap ((v Int)) Int
  (- (mod (+ v 9223372036854775808) 18446744073709551616) 9223372036854775808))

; I2L sign-extends, which keeps the value (JLS 5.1.2); L2I "discards all but the n lowest order
; bits" (JLS 5.1.3).
(define-fun i2l ((x Int)) Int x)
(define-fun l2i ((x Int)) Int (jint.wrap x))

; +, -, * (JLS 15.18.2, 15.17.1), unary - (15.15.4) and ~, where "~x equals (-x)-1" (15.15.5).
(define-fun jint.add ((x Int) (y Int)) Int (jint.wrap (+ x y)))
(define-fun jint.sub ((x Int) (y Int)) Int (jint.wrap (- x y)))
(define-fun jint.mul ((x Int) (y Int)) Int (jint.wrap (* x y)))
(define-fun jint.neg ((x Int)) Int (jint.wrap (- x)))
(define-fun jint.not ((x Int)) Int (- (- x) 1))
(define-fun jlong.add ((x Int) (y Int)) Int (jlong.wrap (+ x y)))
(define-fun jlong.sub ((x Int) (y Int)) Int (jlong.wrap (- x y)))
(define-fun jlong.mul ((x Int) (y Int)) Int (jlong.wrap (* x y)))
(define-fun jlong.neg ((x Int)) Int (jlong.wrap (- x)))
(define-fun jlong.not ((x Int)) Int (- (- x) 1))

; / rounds toward zero, its magnitude as large as possible while |d * q| <= |n|; the one quotient
; out of range, the most negative value divided by -1, "is equal to the dividend" (JLS 15.17.2),
; which the wrap gives. % is what makes "(a/b)*b+(a%b) equal to a" (15.17.3), so
; it takes the sign of the dividend. SMT-LIB's div floors for a positive divisor, hence the
; magnitudes.
(define-fun trunc.div ((x Int) (y Int)) Int
  (ite (= (>= x 0) (> y 0)) (div (abs x) (abs y)) (- (div (abs x) (abs y)))))
(define-fun jint.div ((x Int) (y Int)) Int (jint.wrap (trunc.div x y)))
(define-fun jint.rem ((x Int) (y Int)) Int (- x (* (trunc.div x y) y)))
(define-fun jlong.div ((x Int) (y Int)) Int (jlong.wrap (trunc.div x y)))
(define-fun jlong.rem ((x Int) (y Int)) Int (- x (* (trunc.div x y) y)))

; Math.floorDiv rounds toward negative infinity, with the same overflow as /; Math.floorMod is
; what makes floorDiv(x, y) * y + floorMod(x, y) equal to x, so it takes the sign of the divisor.
(define-fun floor.div ((x Int) (y Int)) Int (ite (> y 0) (div x y) (div (- x) (- y))))
(define-fun jint.floorDiv ((x Int) (y Int)) Int (jint.wrap (floor.div x y)))
(define-fun jint.floorMod ((x Int) (y Int)) Int (- x (* (floor.div x y) y)))
(define-fun jlong.floorDiv ((x Int) (y Int)) Int (jlong.wrap (floor.div x y)))
(define-fun jlong.floorMod ((x Int) (y Int)) Int (- x (* (floor.div x y) y)))

; Shifts (JLS 15.19). Only the five lowest-order bits of an int shift's count are used, and the six
; lowest of a long's: the count modulo 32 or 64. "n << s" is multiplication by 2^s, "n >> s" is
; floor(n / 2^s), and "n >>> s" is n >> s for a non-negative n and (n >> s) + (2 << ~s), or
; (2L << ~s), for a negative one.
(define-fun pow2 ((k Int)) Int
  (ite (= k 0) 1
  (ite (= k 1) 2
  (ite (= k 2) 4
  (ite (= k 3) 8
  (ite (= k 4) 16
  (ite (= k 5) 32
  (ite (= k 6) 64
  (ite (= k 7) 128
  (ite (= k 8) 256
  (ite (= k 9) 512
  (ite (= k 10) 1024
  (ite (= k 11) 2048
  (ite (= k 12) 4096
  (ite (= k 13) 8192
  (ite (= k 14) 16384
  (ite (= k 15) 32768
  (ite (= k 16) 65536
  (ite (= k 17) 131072
  (ite (= k 18) 262144
  (ite (= k 19) 524288
  (ite (= k 20) 1048576
  (ite (= k 21) 2097152
  (ite (= k 22) 4194304
  (ite (= k 23) 8388608
  (ite (= k 24) 16777216
  (ite (= k 25) 33554432
  (ite (= k 26) 67108864
  (ite (= k 27) 134217728
  (ite (= k 28) 268435456
  (ite (= k 29) 536870912
  (ite (= k 30) 1073741824
  (ite (= k 31) 2147483648
  (ite (= k 32) 4294967296
  (ite (= k 33) 8589934592
  (ite (= k 34) 17179869184
  (ite (= k 35) 34359738368
  (ite (= k 36) 68719476736
  (ite (= k 37) 137438953472
  (ite (= k 38) 274877906944
  (ite (= k 39) 549755813888
  (ite (= k 40) 1099511627776
  (ite (= k 41) 2199023255552
  (ite (= k 42) 4398046511104
  (ite (= k 43) 8796093022208
  (ite (= k 44) 17592186044416
  (ite (= k 45) 35184372088832
  (ite (= k 46) 70368744177664
  (ite (= k 47) 140737488355328
  (ite (= k 48) 281474976710656
  (ite (= k 49) 562949953421312
  (ite (= k 50) 1125899906842624
  (ite (= k 51) 2251799813685248
  (ite (= k 52) 4503599627370496
  (ite (= k 53) 9007199254740992
  (ite (= k 54) 18014398509481984
  (ite (= k 55) 36028797018963968
  (ite (= k 56) 72057594037927936
  (ite (= k 57) 144115188075855872
  (ite (= k 58) 288230376151711744
  (ite (= k 59) 576460752303423488
  (ite (= k 60) 1152921504606846976
  (ite (= k 61) 2305843009213693952
  (ite (= k 62) 4611686018427387904
  9223372036854775808))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))
(define-fun jint.shl ((x Int) (s Int)) Int (jint.wrap (* x (pow2 (mod s 32)))))
(define-fun jint.shr ((x Int) (s Int)) Int (div x (pow2 (mod s 32))))
(define-fun jint.ushr ((x Int) (s Int)) Int
  (ite (>= x 0) (jint.shr x s) (jint.add (jint.shr x s) (jint.shl 2 (jint.not s)))))
(define-fun jlong.shl ((x Int) (s Int)) Int (jlong.wrap (* x (pow2 (mod s 64)))))
(define-fun jlong.shr ((x Int) (s Int)) Int (div x (pow2 (mod s 64))))
(define-fun jlong.ushr ((x Int) (s Int)) Int
  (ite (>= x 0) (jlong.shr x s) (jlong.add (jlong.shr x s) (jlong.shl 2 (jlong.not s)))))
