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

; floorMod(v, 7) is what VarkaChronoLowering.emitFloorMod7 computes, in each of its three
; forms, for every int v (VARKA-242). Per lane:
;
;   a fold, VarkaChronoLowering.Fold:     y = (x & (2^k - 1)) + (x >>> k)
;   the shipped form (FloorMod7.MAGIC):   two folds of 15 bits; + 3 where v < 0;
;                                         r = g - ((g * 37450) >>> 18) * 7
;   FloorMod7.DIGIT_SUM:                  folds of 15, 15, 6, 3, 3 and 3 bits; + 3 where
;                                         v < 0; - 7 where that is at least 7
;   FloorMod7.DIV:                        r = v - (v / 7) * 7; + 7 where r < 0
;
; The folds read v unsigned, as u = v or v + 2^32. Each is stated by the digits of its
; input, u = 2^k * a + b with 0 <= b < 2^k, so every check is linear arithmetic over a few
; small variables: a lemma that the prelude's & and >>> give b and a, at both signs; a
; lemma per fold that a + b keeps the residue mod 7, since 2^k is 1 mod 7 for k = 15, 6
; and 3, and is at most the bound written; that + 3 restores a negative v's residue, since
; 2^32 is 4 mod 7; and the last step over the small value the folds leave. A check per
; form then takes the lemmas' conclusions as its premises and shows the result is v's
; residue. A residue kept is written as a difference that is a multiple of 7, and in the
; composition as 7 times a declared integer, with no mod at all: cvc5 decides each of
; these checks alone in milliseconds, but within the file its earlier checks left the mod
; forms of two of them unknown. The file declares ALL: under QF_NIA cvc5 leaves six of
; these checks unknown, under ALL it decides every one (VARKA-242.md 2.2).
;
; Rendered by VarkaProofFiles. VarkaProofFilesSuite fails when this file differs from its
; rendering, and VARKA_PROOFS_REGEN=true build/sbt 'catalyst/testOnly *VarkaProofFilesSuite'
; rewrites it. Run by dev/varka_prove.sh, which inserts java.smt2 after the set-logic line.

(set-logic ALL)

(define-fun fm7.unsigned ((x Int)) Int (ite (< x 0) (+ x 4294967296) x))

; The prelude's & and >>> give a fold's digits.

(echo "x & 32767 is the low digit b of a non-negative int's unsigned reading: expect unsat")
(push 1)
(declare-const x Int)
(declare-const a Int)
(declare-const b Int)
(assert (and (jint.in x) (>= x 0)))
(assert (and (= (fm7.unsigned x) (+ (* 32768 a) b)) (<= 0 b) (< b 32768)))
(assert (not (= (jint.and.low x 32768) b)))
(check-sat)
(pop 1)

(echo "x >>> 15 is the high digit a of a non-negative int's unsigned reading: expect unsat")
(push 1)
(declare-const x Int)
(declare-const a Int)
(declare-const b Int)
(assert (and (jint.in x) (>= x 0)))
(assert (and (= (fm7.unsigned x) (+ (* 32768 a) b)) (<= 0 b) (< b 32768)))
(assert (not (= (jint.ushr x 15) a)))
(check-sat)
(pop 1)

(echo "x & 32767 is the low digit b of a negative int's unsigned reading: expect unsat")
(push 1)
(declare-const x Int)
(declare-const a Int)
(declare-const b Int)
(assert (and (jint.in x) (< x 0)))
(assert (and (= (fm7.unsigned x) (+ (* 32768 a) b)) (<= 0 b) (< b 32768)))
(assert (not (= (jint.and.low x 32768) b)))
(check-sat)
(pop 1)

(echo "x >>> 15 is the high digit a of a negative int's unsigned reading: expect unsat")
(push 1)
(declare-const x Int)
(declare-const a Int)
(declare-const b Int)
(assert (and (jint.in x) (< x 0)))
(assert (and (= (fm7.unsigned x) (+ (* 32768 a) b)) (<= 0 b) (< b 32768)))
(assert (not (= (jint.ushr x 15) a)))
(check-sat)
(pop 1)

(echo "x & 63 is the low digit b of a non-negative int's unsigned reading: expect unsat")
(push 1)
(declare-const x Int)
(declare-const a Int)
(declare-const b Int)
(assert (and (jint.in x) (>= x 0)))
(assert (and (= (fm7.unsigned x) (+ (* 64 a) b)) (<= 0 b) (< b 64)))
(assert (not (= (jint.and.low x 64) b)))
(check-sat)
(pop 1)

(echo "x >>> 6 is the high digit a of a non-negative int's unsigned reading: expect unsat")
(push 1)
(declare-const x Int)
(declare-const a Int)
(declare-const b Int)
(assert (and (jint.in x) (>= x 0)))
(assert (and (= (fm7.unsigned x) (+ (* 64 a) b)) (<= 0 b) (< b 64)))
(assert (not (= (jint.ushr x 6) a)))
(check-sat)
(pop 1)

(echo "x & 63 is the low digit b of a negative int's unsigned reading: expect unsat")
(push 1)
(declare-const x Int)
(declare-const a Int)
(declare-const b Int)
(assert (and (jint.in x) (< x 0)))
(assert (and (= (fm7.unsigned x) (+ (* 64 a) b)) (<= 0 b) (< b 64)))
(assert (not (= (jint.and.low x 64) b)))
(check-sat)
(pop 1)

(echo "x >>> 6 is the high digit a of a negative int's unsigned reading: expect unsat")
(push 1)
(declare-const x Int)
(declare-const a Int)
(declare-const b Int)
(assert (and (jint.in x) (< x 0)))
(assert (and (= (fm7.unsigned x) (+ (* 64 a) b)) (<= 0 b) (< b 64)))
(assert (not (= (jint.ushr x 6) a)))
(check-sat)
(pop 1)

(echo "x & 7 is the low digit b of a non-negative int's unsigned reading: expect unsat")
(push 1)
(declare-const x Int)
(declare-const a Int)
(declare-const b Int)
(assert (and (jint.in x) (>= x 0)))
(assert (and (= (fm7.unsigned x) (+ (* 8 a) b)) (<= 0 b) (< b 8)))
(assert (not (= (jint.and.low x 8) b)))
(check-sat)
(pop 1)

(echo "x >>> 3 is the high digit a of a non-negative int's unsigned reading: expect unsat")
(push 1)
(declare-const x Int)
(declare-const a Int)
(declare-const b Int)
(assert (and (jint.in x) (>= x 0)))
(assert (and (= (fm7.unsigned x) (+ (* 8 a) b)) (<= 0 b) (< b 8)))
(assert (not (= (jint.ushr x 3) a)))
(check-sat)
(pop 1)

(echo "x & 7 is the low digit b of a negative int's unsigned reading: expect unsat")
(push 1)
(declare-const x Int)
(declare-const a Int)
(declare-const b Int)
(assert (and (jint.in x) (< x 0)))
(assert (and (= (fm7.unsigned x) (+ (* 8 a) b)) (<= 0 b) (< b 8)))
(assert (not (= (jint.and.low x 8) b)))
(check-sat)
(pop 1)

(echo "x >>> 3 is the high digit a of a negative int's unsigned reading: expect unsat")
(push 1)
(declare-const x Int)
(declare-const a Int)
(declare-const b Int)
(assert (and (jint.in x) (< x 0)))
(assert (and (= (fm7.unsigned x) (+ (* 8 a) b)) (<= 0 b) (< b 8)))
(assert (not (= (jint.ushr x 3) a)))
(check-sat)
(pop 1)

; the shipped form's folds keep the residue mod 7.

(echo "the shipped form: a fold of 15 bits over [0, 4294967295] is at most 163838 and keeps the residue: expect unsat")
(push 1)
(declare-const y Int)
(declare-const a Int)
(declare-const b Int)
(assert (and (<= 0 y) (<= y 4294967295) (= y (+ (* 32768 a) b)) (<= 0 b) (< b 32768)))
(assert (not (and (<= (+ a b) 163838) (= (mod (- y (+ a b)) 7) 0))))
(check-sat)
(pop 1)

(echo "the shipped form: a fold of 15 bits over [0, 163838] is at most 32770 and keeps the residue: expect unsat")
(push 1)
(declare-const y Int)
(declare-const a Int)
(declare-const b Int)
(assert (and (<= 0 y) (<= y 163838) (= y (+ (* 32768 a) b)) (<= 0 b) (< b 32768)))
(assert (not (and (<= (+ a b) 32770) (= (mod (- y (+ a b)) 7) 0))))
(check-sat)
(pop 1)

; DIGIT_SUM's folds keep the residue mod 7.

(echo "DIGIT_SUM: a fold of 15 bits over [0, 4294967295] is at most 163838 and keeps the residue: expect unsat")
(push 1)
(declare-const y Int)
(declare-const a Int)
(declare-const b Int)
(assert (and (<= 0 y) (<= y 4294967295) (= y (+ (* 32768 a) b)) (<= 0 b) (< b 32768)))
(assert (not (and (<= (+ a b) 163838) (= (mod (- y (+ a b)) 7) 0))))
(check-sat)
(pop 1)

(echo "DIGIT_SUM: a fold of 15 bits over [0, 163838] is at most 32770 and keeps the residue: expect unsat")
(push 1)
(declare-const y Int)
(declare-const a Int)
(declare-const b Int)
(assert (and (<= 0 y) (<= y 163838) (= y (+ (* 32768 a) b)) (<= 0 b) (< b 32768)))
(assert (not (and (<= (+ a b) 32770) (= (mod (- y (+ a b)) 7) 0))))
(check-sat)
(pop 1)

(echo "DIGIT_SUM: a fold of 6 bits over [0, 32770] is at most 574 and keeps the residue: expect unsat")
(push 1)
(declare-const y Int)
(declare-const a Int)
(declare-const b Int)
(assert (and (<= 0 y) (<= y 32770) (= y (+ (* 64 a) b)) (<= 0 b) (< b 64)))
(assert (not (and (<= (+ a b) 574) (= (mod (- y (+ a b)) 7) 0))))
(check-sat)
(pop 1)

(echo "DIGIT_SUM: a fold of 3 bits over [0, 574] is at most 77 and keeps the residue: expect unsat")
(push 1)
(declare-const y Int)
(declare-const a Int)
(declare-const b Int)
(assert (and (<= 0 y) (<= y 574) (= y (+ (* 8 a) b)) (<= 0 b) (< b 8)))
(assert (not (and (<= (+ a b) 77) (= (mod (- y (+ a b)) 7) 0))))
(check-sat)
(pop 1)

(echo "DIGIT_SUM: a fold of 3 bits over [0, 77] is at most 15 and keeps the residue: expect unsat")
(push 1)
(declare-const y Int)
(declare-const a Int)
(declare-const b Int)
(assert (and (<= 0 y) (<= y 77) (= y (+ (* 8 a) b)) (<= 0 b) (< b 8)))
(assert (not (and (<= (+ a b) 15) (= (mod (- y (+ a b)) 7) 0))))
(check-sat)
(pop 1)

(echo "DIGIT_SUM: a fold of 3 bits over [0, 15] is at most 8 and keeps the residue: expect unsat")
(push 1)
(declare-const y Int)
(declare-const a Int)
(declare-const b Int)
(assert (and (<= 0 y) (<= y 15) (= y (+ (* 8 a) b)) (<= 0 b) (< b 8)))
(assert (not (and (<= (+ a b) 8) (= (mod (- y (+ a b)) 7) 0))))
(check-sat)
(pop 1)

; The sign fixup, and the last steps.

(echo "+ 3 restores a negative int's residue from its unsigned reading: expect unsat")
(push 1)
(declare-const x Int)
(assert (and (jint.in x) (< x 0)))
(assert (not (= (mod (- (+ (fm7.unsigned x) 3) x) 7) 0)))
(check-sat)
(pop 1)

(echo "+ 3 does not wrap over [0, 32770]: expect unsat")
(push 1)
(declare-const y Int)
(assert (and (<= 0 y) (<= y 32770)))
(assert (not (= (jint.add y 3) (+ y 3))))
(check-sat)
(pop 1)

(echo "+ 3 does not wrap over [0, 8]: expect unsat")
(push 1)
(declare-const y Int)
(assert (and (<= 0 y) (<= y 8)))
(assert (not (= (jint.add y 3) (+ y 3))))
(check-sat)
(pop 1)

(echo "the shipped form: (g * 37450) >>> 18 is g / 7 over [0, 32773]: expect unsat")
(push 1)
(declare-const g Int)
(assert (and (<= 0 g) (<= g 32773)))
(assert (not (= (jint.ushr (jint.mul g 37450) 18) (div g 7))))
(check-sat)
(pop 1)

(echo "the shipped form: g - (g / 7) * 7 is g's residue over [0, 32773]: expect unsat")
(push 1)
(declare-const g Int)
(assert (and (<= 0 g) (<= g 32773)))
(assert (not (= (jint.sub g (jint.mul (div g 7) 7)) (mod g 7))))
(check-sat)
(pop 1)

(echo "DIGIT_SUM: one subtract of 7 where at least 7 is the residue over [0, 11]: expect unsat")
(push 1)
(declare-const g Int)
(assert (and (<= 0 g) (<= g 11)))
(assert (not (= (ite (>= g 7) (jint.sub g 7) g) (mod g 7))))
(check-sat)
(pop 1)

; Each form composed: the lemmas' conclusions as premises.

(echo "the shipped form: the folds, the fixup and the last step give v's residue: expect unsat")
(push 1)
(declare-const x Int)
(assert (jint.in x))
(declare-const y0 Int) (declare-const c0 Int)
(assert (and (<= 0 y0) (<= y0 163838) (= (- (fm7.unsigned x) y0) (* 7 c0))))
(declare-const y1 Int) (declare-const c1 Int)
(assert (and (<= 0 y1) (<= y1 32770) (= (- y0 y1) (* 7 c1))))
(define-fun g () Int (ite (< x 0) (+ y1 3) y1))
(declare-const cf Int)
(assert (=> (< x 0) (= (- (+ (fm7.unsigned x) 3) x) (* 7 cf))))
; mod g 7 and mod x 7, as the residues sg and sx of their own quotients
(declare-const qg Int) (declare-const sg Int)
(assert (and (= g (+ (* 7 qg) sg)) (<= 0 sg) (< sg 7)))
(declare-const qx Int) (declare-const sx Int)
(assert (and (= x (+ (* 7 qx) sx)) (<= 0 sx) (< sx 7)))
(assert (not (= sg sx)))
(check-sat)
(pop 1)

(echo "DIGIT_SUM: the folds, the fixup and the last step give v's residue: expect unsat")
(push 1)
(declare-const x Int)
(assert (jint.in x))
(declare-const y0 Int) (declare-const c0 Int)
(assert (and (<= 0 y0) (<= y0 163838) (= (- (fm7.unsigned x) y0) (* 7 c0))))
(declare-const y1 Int) (declare-const c1 Int)
(assert (and (<= 0 y1) (<= y1 32770) (= (- y0 y1) (* 7 c1))))
(declare-const y2 Int) (declare-const c2 Int)
(assert (and (<= 0 y2) (<= y2 574) (= (- y1 y2) (* 7 c2))))
(declare-const y3 Int) (declare-const c3 Int)
(assert (and (<= 0 y3) (<= y3 77) (= (- y2 y3) (* 7 c3))))
(declare-const y4 Int) (declare-const c4 Int)
(assert (and (<= 0 y4) (<= y4 15) (= (- y3 y4) (* 7 c4))))
(declare-const y5 Int) (declare-const c5 Int)
(assert (and (<= 0 y5) (<= y5 8) (= (- y4 y5) (* 7 c5))))
(define-fun g () Int (ite (< x 0) (+ y5 3) y5))
(declare-const cf Int)
(assert (=> (< x 0) (= (- (+ (fm7.unsigned x) 3) x) (* 7 cf))))
; mod g 7 and mod x 7, as the residues sg and sx of their own quotients
(declare-const qg Int) (declare-const sg Int)
(assert (and (= g (+ (* 7 qg) sg)) (<= 0 sg) (< sg 7)))
(declare-const qx Int) (declare-const sx Int)
(assert (and (= x (+ (* 7 qx) sx)) (<= 0 sx) (< sx 7)))
(assert (not (= sg sx)))
(check-sat)
(pop 1)

(echo "DIV: v / 7 is the quotient truncated toward zero, for a non-negative int: expect unsat")
(push 1)
(declare-const x Int) (declare-const q Int) (declare-const t Int)
(assert (and (jint.in x) (>= x 0)))
(assert (and (= x (+ (* 7 q) t)) (<= 0 t) (< t 7)))
(assert (not (= (jint.div x 7) q)))
(check-sat)
(pop 1)

(echo "DIV: v - (v / 7) * 7 is the remainder t, without wrapping, for a non-negative int: expect unsat")
(push 1)
(declare-const x Int) (declare-const q Int) (declare-const t Int)
(assert (and (jint.in x) (>= x 0)))
(assert (and (= x (+ (* 7 q) t)) (<= 0 t) (< t 7)))
(assert (not (= (jint.sub x (jint.mul q 7)) t)))
(check-sat)
(pop 1)

(echo "DIV: t, + 7 where negative, is the residue, for a non-negative int: expect unsat")
(push 1)
(declare-const x Int) (declare-const q Int) (declare-const t Int)
(assert (and (jint.in x) (>= x 0)))
(assert (and (= x (+ (* 7 q) t)) (<= 0 t) (< t 7)))
(declare-const qx Int) (declare-const sx Int)
(assert (and (= x (+ (* 7 qx) sx)) (<= 0 sx) (< sx 7)))
(assert (not (= (ite (< t 0) (jint.add t 7) t) sx)))
(check-sat)
(pop 1)

(echo "DIV: v / 7 is the quotient truncated toward zero, for a negative int: expect unsat")
(push 1)
(declare-const x Int) (declare-const q Int) (declare-const t Int)
(assert (and (jint.in x) (< x 0)))
(assert (and (= x (+ (* 7 q) t)) (< (- 7) t) (<= t 0)))
(assert (not (= (jint.div x 7) q)))
(check-sat)
(pop 1)

(echo "DIV: v - (v / 7) * 7 is the remainder t, without wrapping, for a negative int: expect unsat")
(push 1)
(declare-const x Int) (declare-const q Int) (declare-const t Int)
(assert (and (jint.in x) (< x 0)))
(assert (and (= x (+ (* 7 q) t)) (< (- 7) t) (<= t 0)))
(assert (not (= (jint.sub x (jint.mul q 7)) t)))
(check-sat)
(pop 1)

(echo "DIV: t, + 7 where negative, is the residue, for a negative int: expect unsat")
(push 1)
(declare-const x Int) (declare-const q Int) (declare-const t Int)
(assert (and (jint.in x) (< x 0)))
(assert (and (= x (+ (* 7 q) t)) (< (- 7) t) (<= t 0)))
(declare-const qx Int) (declare-const sx Int)
(assert (and (= x (+ (* 7 qx) sx)) (<= 0 sx) (< sx 7)))
(assert (not (= (ite (< t 0) (jint.add t 7) t) sx)))
(check-sat)
(pop 1)

(echo "Math.floorMod(v, 7) is v's non-negative residue for every int: expect unsat")
(push 1)
(declare-const x Int)
(assert (jint.in x))
(declare-const qx Int) (declare-const sx Int)
(assert (and (= x (+ (* 7 qx) sx)) (<= 0 sx) (< sx 7)))
(assert (not (= (jint.floorMod x 7) sx)))
(check-sat)
(pop 1)
