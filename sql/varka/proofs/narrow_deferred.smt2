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

; The deferred wrap of the widened arm under LEGACY (VARKA-330, VARKA-329.md 4.2). Only
; +, -, *, the bitwise operations and a left shift commute with reduction mod 2^w; every
; other consumer - a compare, abs, a greatest, a widening, an IfElse's condition, the
; store - reads the reduced value, so the wrap happens there and not at the interior
; node:
;
;   wrap(u op v) = wrap(wrap(u) op wrap(v))        op in { +, -, * }
;
; for every int u and v: the wrap is a ring homomorphism onto the w-bit values, and the
; int lane's own wraparound is a second reduction that does not change the residue.
; Induction over a tree of ring nodes follows. The sat checks place the law at the int
; lane's ends and show the set is not larger: a compare of an unreduced value differs
; from the compare of its reduction, which is why it forces the wrap.
;
; The logic is ALL because the file reasons over bit-vectors.
;
; Rendered by VarkaProofFiles. VarkaProofFilesSuite fails when this file differs from its
; rendering, and VARKA_PROOFS_REGEN=true build/sbt 'catalyst/testOnly *VarkaProofFilesSuite'
; rewrites it. Run by dev/varka_prove.sh, which inserts java.smt2 after the set-logic line.

(set-logic ALL)

(echo "narrow_deferred: reducing after the int lane's add is reducing the operands first, at 8 bits: expect unsat")
(push 1)
(declare-const u (_ BitVec 32))
(declare-const v (_ BitVec 32))
(assert (not (= ((_ sign_extend 24) ((_ extract 7 0) (bvadd u v))) ((_ sign_extend 24) ((_ extract 7 0) (bvadd ((_ sign_extend 24) ((_ extract 7 0) u)) ((_ sign_extend 24) ((_ extract 7 0) v))))))))
(check-sat)
(pop 1)

(echo "narrow_deferred: reducing after the int lane's sub is reducing the operands first, at 8 bits: expect unsat")
(push 1)
(declare-const u (_ BitVec 32))
(declare-const v (_ BitVec 32))
(assert (not (= ((_ sign_extend 24) ((_ extract 7 0) (bvsub u v))) ((_ sign_extend 24) ((_ extract 7 0) (bvsub ((_ sign_extend 24) ((_ extract 7 0) u)) ((_ sign_extend 24) ((_ extract 7 0) v))))))))
(check-sat)
(pop 1)

(echo "narrow_deferred: reducing after the int lane's mul is reducing the operands first, at 8 bits: expect unsat")
(push 1)
(declare-const u (_ BitVec 32))
(declare-const v (_ BitVec 32))
(assert (not (= ((_ sign_extend 24) ((_ extract 7 0) (bvmul u v))) ((_ sign_extend 24) ((_ extract 7 0) (bvmul ((_ sign_extend 24) ((_ extract 7 0) u)) ((_ sign_extend 24) ((_ extract 7 0) v))))))))
(check-sat)
(pop 1)

(echo "narrow_deferred: the law holds at the int lane's two ends, over byte bounds: expect sat")
(push 1)
(declare-const u (_ BitVec 32))
(declare-const v (_ BitVec 32))
(assert (= u #x80000000))
(assert (= v #x7fffffff))
(assert (= ((_ sign_extend 24) ((_ extract 7 0) (bvmul u v))) ((_ sign_extend 24) ((_ extract 7 0) (bvmul ((_ sign_extend 24) ((_ extract 7 0) u)) ((_ sign_extend 24) ((_ extract 7 0) v)))))))
(check-sat)
(pop 1)

(echo "narrow_deferred: a compare is not in the ring, and an unreduced value changes it: expect sat")
(push 1)
(declare-const u (_ BitVec 32))
(declare-const v (_ BitVec 32))
(declare-const c (_ BitVec 32))
(assert (= u #x00000080))
(assert (= c #x00000000))
(assert (not (= (bvslt u c) (bvslt ((_ sign_extend 24) ((_ extract 7 0) u)) c))))
(check-sat)
(pop 1)

(echo "narrow_deferred: the byte bounds are ints the law leaves alone: expect sat")
(push 1)
(declare-const s (_ BitVec 32))
(declare-const t (_ BitVec 32))
(assert (= s #xffffff80))
(assert (= t #x0000007f))
(assert (= ((_ sign_extend 24) ((_ extract 7 0) s)) s))
(assert (= ((_ sign_extend 24) ((_ extract 7 0) t)) t))
(check-sat)
(pop 1)

(echo "narrow_deferred: reducing after the int lane's add is reducing the operands first, at 16 bits: expect unsat")
(push 1)
(declare-const u (_ BitVec 32))
(declare-const v (_ BitVec 32))
(assert (not (= ((_ sign_extend 16) ((_ extract 15 0) (bvadd u v))) ((_ sign_extend 16) ((_ extract 15 0) (bvadd ((_ sign_extend 16) ((_ extract 15 0) u)) ((_ sign_extend 16) ((_ extract 15 0) v))))))))
(check-sat)
(pop 1)

(echo "narrow_deferred: reducing after the int lane's sub is reducing the operands first, at 16 bits: expect unsat")
(push 1)
(declare-const u (_ BitVec 32))
(declare-const v (_ BitVec 32))
(assert (not (= ((_ sign_extend 16) ((_ extract 15 0) (bvsub u v))) ((_ sign_extend 16) ((_ extract 15 0) (bvsub ((_ sign_extend 16) ((_ extract 15 0) u)) ((_ sign_extend 16) ((_ extract 15 0) v))))))))
(check-sat)
(pop 1)

(echo "narrow_deferred: reducing after the int lane's mul is reducing the operands first, at 16 bits: expect unsat")
(push 1)
(declare-const u (_ BitVec 32))
(declare-const v (_ BitVec 32))
(assert (not (= ((_ sign_extend 16) ((_ extract 15 0) (bvmul u v))) ((_ sign_extend 16) ((_ extract 15 0) (bvmul ((_ sign_extend 16) ((_ extract 15 0) u)) ((_ sign_extend 16) ((_ extract 15 0) v))))))))
(check-sat)
(pop 1)

(echo "narrow_deferred: the law holds at the int lane's two ends, over short bounds: expect sat")
(push 1)
(declare-const u (_ BitVec 32))
(declare-const v (_ BitVec 32))
(assert (= u #x80000000))
(assert (= v #x7fffffff))
(assert (= ((_ sign_extend 16) ((_ extract 15 0) (bvmul u v))) ((_ sign_extend 16) ((_ extract 15 0) (bvmul ((_ sign_extend 16) ((_ extract 15 0) u)) ((_ sign_extend 16) ((_ extract 15 0) v)))))))
(check-sat)
(pop 1)

(echo "narrow_deferred: a compare is not in the ring, and an unreduced value changes it: expect sat")
(push 1)
(declare-const u (_ BitVec 32))
(declare-const v (_ BitVec 32))
(declare-const c (_ BitVec 32))
(assert (= u #x00008000))
(assert (= c #x00000000))
(assert (not (= (bvslt u c) (bvslt ((_ sign_extend 16) ((_ extract 15 0) u)) c))))
(check-sat)
(pop 1)

(echo "narrow_deferred: the short bounds are ints the law leaves alone: expect sat")
(push 1)
(declare-const s (_ BitVec 32))
(declare-const t (_ BitVec 32))
(assert (= s #xffff8000))
(assert (= t #x00007fff))
(assert (= ((_ sign_extend 16) ((_ extract 15 0) s)) s))
(assert (= ((_ sign_extend 16) ((_ extract 15 0) t)) t))
(check-sat)
(pop 1)
