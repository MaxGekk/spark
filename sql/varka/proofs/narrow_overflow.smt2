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

; The checked narrow add and subtract on the native arm (VARKA-330, VARKA-329.md 4.3).
; The lanes are the narrow ones, so the wrapping operator is the lane's own add or
; subtract, and the mask is one more lanewise - the saturating form - and a compare:
;
;   overflow(a, b) = bvadd(a, b) != sadd(a, b)          subtract with ssub
;
; The claims: that mask is set at exactly the lanes where the exact result leaves
; [-2^(w-1), 2^(w-1) - 1]. The exact result is formed in a lane one bit wider, where
; the sum of two w-bit values cannot wrap, so "outside the range" there is a comparison
; and not another way of stating the overflow. The saturating forms are SMT-LIB's
; signed-overflow predicate and the saturated constant, which is what the Vector API's
; SADD and SSUB compute. Beside the claims, the domain's ends, and the neighbours: the
; result's sign alone is not the mask, and the other operator's saturating form is not
; the mask either.
;
; The logic is ALL because the file reasons over bit-vectors.
;
; Rendered by VarkaProofFiles. VarkaProofFilesSuite fails when this file differs from its
; rendering, and VARKA_PROOFS_REGEN=true build/sbt 'catalyst/testOnly *VarkaProofFilesSuite'
; rewrites it. Run by dev/varka_prove.sh, which inserts java.smt2 after the set-logic line.

(set-logic ALL)

(echo "narrow_overflow: the byte sum's mask is the exact sum leaving the range: expect unsat")
(push 1)
(declare-const a (_ BitVec 8))
(declare-const b (_ BitVec 8))
(assert (not (= (not (= (bvadd a b) (ite (bvsaddo a b) (ite (bvslt a #x00) #x80 #x7f) (bvadd a b)))) (or (bvslt (bvadd ((_ sign_extend 1) a) ((_ sign_extend 1) b)) #b110000000) (bvsgt (bvadd ((_ sign_extend 1) a) ((_ sign_extend 1) b)) #b001111111)))))
(check-sat)
(pop 1)

(echo "narrow_overflow: the byte difference's mask is the exact one leaving the range: expect unsat")
(push 1)
(declare-const a (_ BitVec 8))
(declare-const b (_ BitVec 8))
(assert (not (= (not (= (bvsub a b) (ite (bvssubo a b) (ite (bvslt a #x00) #x80 #x7f) (bvsub a b)))) (or (bvslt (bvsub ((_ sign_extend 1) a) ((_ sign_extend 1) b)) #b110000000) (bvsgt (bvsub ((_ sign_extend 1) a) ((_ sign_extend 1) b)) #b001111111)))))
(check-sat)
(pop 1)

(echo "narrow_overflow: the domain admits the top of the byte range, where nothing overflows: expect sat")
(push 1)
(declare-const a (_ BitVec 8))
(declare-const b (_ BitVec 8))
(assert (= a #x7f))
(assert (= b #x00))
(assert (= (bvadd a b) (ite (bvsaddo a b) (ite (bvslt a #x00) #x80 #x7f) (bvadd a b))))
(check-sat)
(pop 1)

(echo "narrow_overflow: one past the top is outside the range, and the byte's add and sadd differ: expect sat")
(push 1)
(declare-const a (_ BitVec 8))
(declare-const b (_ BitVec 8))
(assert (= a #x7f))
(assert (= b #x01))
(assert (not (= (bvadd a b) (ite (bvsaddo a b) (ite (bvslt a #x00) #x80 #x7f) (bvadd a b)))))
(assert (= (bvadd ((_ sign_extend 1) a) ((_ sign_extend 1) b)) #b010000000))
(check-sat)
(pop 1)

(echo "narrow_overflow: one past the bottom of the byte range is outside it, and subtract differs: expect sat")
(push 1)
(declare-const a (_ BitVec 8))
(declare-const b (_ BitVec 8))
(assert (= a #x80))
(assert (= b #x01))
(assert (not (= (bvsub a b) (ite (bvssubo a b) (ite (bvslt a #x00) #x80 #x7f) (bvsub a b)))))
(check-sat)
(pop 1)

(echo "narrow_overflow: the result's sign alone is not the sum's mask: expect sat")
(push 1)
(declare-const a (_ BitVec 8))
(declare-const b (_ BitVec 8))
(assert (not (or (bvslt (bvadd ((_ sign_extend 1) a) ((_ sign_extend 1) b)) #b110000000) (bvsgt (bvadd ((_ sign_extend 1) a) ((_ sign_extend 1) b)) #b001111111))))
(assert (bvslt (bvadd a b) #x00))
(check-sat)
(pop 1)

(echo "narrow_overflow: the other operator's saturating form is not the sum's mask: expect sat")
(push 1)
(declare-const a (_ BitVec 8))
(declare-const b (_ BitVec 8))
(assert (not (or (bvslt (bvadd ((_ sign_extend 1) a) ((_ sign_extend 1) b)) #b110000000) (bvsgt (bvadd ((_ sign_extend 1) a) ((_ sign_extend 1) b)) #b001111111))))
(assert (not (= (bvadd a b) (ite (bvssubo a b) (ite (bvslt a #x00) #x80 #x7f) (bvsub a b)))))
(check-sat)
(pop 1)

(echo "narrow_overflow: the short sum's mask is the exact sum leaving the range: expect unsat")
(push 1)
(declare-const a (_ BitVec 16))
(declare-const b (_ BitVec 16))
(assert (not (= (not (= (bvadd a b) (ite (bvsaddo a b) (ite (bvslt a #x0000) #x8000 #x7fff) (bvadd a b)))) (or (bvslt (bvadd ((_ sign_extend 1) a) ((_ sign_extend 1) b)) #b11000000000000000) (bvsgt (bvadd ((_ sign_extend 1) a) ((_ sign_extend 1) b)) #b00111111111111111)))))
(check-sat)
(pop 1)

(echo "narrow_overflow: the short difference's mask is the exact one leaving the range: expect unsat")
(push 1)
(declare-const a (_ BitVec 16))
(declare-const b (_ BitVec 16))
(assert (not (= (not (= (bvsub a b) (ite (bvssubo a b) (ite (bvslt a #x0000) #x8000 #x7fff) (bvsub a b)))) (or (bvslt (bvsub ((_ sign_extend 1) a) ((_ sign_extend 1) b)) #b11000000000000000) (bvsgt (bvsub ((_ sign_extend 1) a) ((_ sign_extend 1) b)) #b00111111111111111)))))
(check-sat)
(pop 1)

(echo "narrow_overflow: the domain admits the top of the short range, where nothing overflows: expect sat")
(push 1)
(declare-const a (_ BitVec 16))
(declare-const b (_ BitVec 16))
(assert (= a #x7fff))
(assert (= b #x0000))
(assert (= (bvadd a b) (ite (bvsaddo a b) (ite (bvslt a #x0000) #x8000 #x7fff) (bvadd a b))))
(check-sat)
(pop 1)

(echo "narrow_overflow: one past the top is outside the range, and the short's add and sadd differ: expect sat")
(push 1)
(declare-const a (_ BitVec 16))
(declare-const b (_ BitVec 16))
(assert (= a #x7fff))
(assert (= b #x0001))
(assert (not (= (bvadd a b) (ite (bvsaddo a b) (ite (bvslt a #x0000) #x8000 #x7fff) (bvadd a b)))))
(assert (= (bvadd ((_ sign_extend 1) a) ((_ sign_extend 1) b)) #b01000000000000000))
(check-sat)
(pop 1)

(echo "narrow_overflow: one past the bottom of the short range is outside it, and subtract differs: expect sat")
(push 1)
(declare-const a (_ BitVec 16))
(declare-const b (_ BitVec 16))
(assert (= a #x8000))
(assert (= b #x0001))
(assert (not (= (bvsub a b) (ite (bvssubo a b) (ite (bvslt a #x0000) #x8000 #x7fff) (bvsub a b)))))
(check-sat)
(pop 1)

(echo "narrow_overflow: the result's sign alone is not the sum's mask: expect sat")
(push 1)
(declare-const a (_ BitVec 16))
(declare-const b (_ BitVec 16))
(assert (not (or (bvslt (bvadd ((_ sign_extend 1) a) ((_ sign_extend 1) b)) #b11000000000000000) (bvsgt (bvadd ((_ sign_extend 1) a) ((_ sign_extend 1) b)) #b00111111111111111))))
(assert (bvslt (bvadd a b) #x0000))
(check-sat)
(pop 1)

(echo "narrow_overflow: the other operator's saturating form is not the sum's mask: expect sat")
(push 1)
(declare-const a (_ BitVec 16))
(declare-const b (_ BitVec 16))
(assert (not (or (bvslt (bvadd ((_ sign_extend 1) a) ((_ sign_extend 1) b)) #b11000000000000000) (bvsgt (bvadd ((_ sign_extend 1) a) ((_ sign_extend 1) b)) #b00111111111111111))))
(assert (not (= (bvadd a b) (ite (bvssubo a b) (ite (bvslt a #x0000) #x8000 #x7fff) (bvsub a b)))))
(check-sat)
(pop 1)
