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

; The checked narrow forms on the widened arm (VARKA-330, VARKA-329.md 4.2). The int
; lane's result of two narrow operands is exact, so the node's own check is two compares
; against the type's range, whose ends are the lane's (Lane.mostNegative and
; mostPositive, read here):
;
;   outside(r) = r < min || r > max
;
; The claim: outside(r) holds at exactly the ints that are not values of the narrow
; type, which is r differing from the sign extension of its low w bits. The sat checks
; place both ends inside and one past each end outside, and the neighbours narrow the
; compares to <= and >=, which is wrong at the end each names. The native arm multiplies
; a checked narrow pair in the next lane up (byte in short, short in int); the same
; compare over that lane's values, which is where the product lands, is the same test.
;
; The logic is ALL because the file reasons over bit-vectors.
;
; Rendered by VarkaProofFiles. VarkaProofFilesSuite fails when this file differs from its
; rendering, and VARKA_PROOFS_REGEN=true build/sbt 'catalyst/testOnly *VarkaProofFilesSuite'
; rewrites it. Run by dev/varka_prove.sh, which inserts java.smt2 after the set-logic line.

(set-logic ALL)

(echo "narrow_range: the widened arm's mask is the ints that are not byte values: expect unsat")
(push 1)
(declare-const r (_ BitVec 32))
(assert (not (= (or (bvslt r #xffffff80) (bvsgt r #x0000007f)) (not (= r ((_ sign_extend 24) ((_ extract 7 0) r)))))))
(check-sat)
(pop 1)

(echo "narrow_range: the byte range admits both of its ends: expect sat")
(push 1)
(declare-const bottom (_ BitVec 32))
(declare-const top (_ BitVec 32))
(assert (= bottom #xffffff80))
(assert (= top #x0000007f))
(assert (not (or (bvslt bottom #xffffff80) (bvsgt bottom #x0000007f))))
(assert (not (or (bvslt top #xffffff80) (bvsgt top #x0000007f))))
(check-sat)
(pop 1)

(echo "narrow_range: one past each end of the byte is outside it: expect sat")
(push 1)
(declare-const below (_ BitVec 32))
(declare-const above (_ BitVec 32))
(assert (= below #xffffff7f))
(assert (= above #x00000080))
(assert (or (bvslt below #xffffff80) (bvsgt below #x0000007f)))
(assert (or (bvslt above #xffffff80) (bvsgt above #x0000007f)))
(check-sat)
(pop 1)

(echo "narrow_range: with <= at the bottom the mask is not the byte's values: expect sat")
(push 1)
(declare-const r (_ BitVec 32))
(assert (= r #xffffff80))
(assert (not (= (or (bvsle r #xffffff80) (bvsgt r #x0000007f)) (or (bvslt r #xffffff80) (bvsgt r #x0000007f)))))
(check-sat)
(pop 1)

(echo "narrow_range: with >= at the top the mask is not the byte's values: expect sat")
(push 1)
(declare-const r (_ BitVec 32))
(assert (= r #x0000007f))
(assert (not (= (or (bvslt r #xffffff80) (bvsge r #x0000007f)) (or (bvslt r #xffffff80) (bvsgt r #x0000007f)))))
(check-sat)
(pop 1)

(echo "narrow_range: the native arm's multiply, in the 16-bit lane, is checked by the same mask: expect unsat")
(push 1)
(declare-const p (_ BitVec 16))
(assert (not (= (or (bvslt p #xff80) (bvsgt p #x007f)) (not (= p ((_ sign_extend 8) ((_ extract 7 0) p)))))))
(check-sat)
(pop 1)

(echo "narrow_range: the widened arm's mask is the ints that are not short values: expect unsat")
(push 1)
(declare-const r (_ BitVec 32))
(assert (not (= (or (bvslt r #xffff8000) (bvsgt r #x00007fff)) (not (= r ((_ sign_extend 16) ((_ extract 15 0) r)))))))
(check-sat)
(pop 1)

(echo "narrow_range: the short range admits both of its ends: expect sat")
(push 1)
(declare-const bottom (_ BitVec 32))
(declare-const top (_ BitVec 32))
(assert (= bottom #xffff8000))
(assert (= top #x00007fff))
(assert (not (or (bvslt bottom #xffff8000) (bvsgt bottom #x00007fff))))
(assert (not (or (bvslt top #xffff8000) (bvsgt top #x00007fff))))
(check-sat)
(pop 1)

(echo "narrow_range: one past each end of the short is outside it: expect sat")
(push 1)
(declare-const below (_ BitVec 32))
(declare-const above (_ BitVec 32))
(assert (= below #xffff7fff))
(assert (= above #x00008000))
(assert (or (bvslt below #xffff8000) (bvsgt below #x00007fff)))
(assert (or (bvslt above #xffff8000) (bvsgt above #x00007fff)))
(check-sat)
(pop 1)

(echo "narrow_range: with <= at the bottom the mask is not the short's values: expect sat")
(push 1)
(declare-const r (_ BitVec 32))
(assert (= r #xffff8000))
(assert (not (= (or (bvsle r #xffff8000) (bvsgt r #x00007fff)) (or (bvslt r #xffff8000) (bvsgt r #x00007fff)))))
(check-sat)
(pop 1)

(echo "narrow_range: with >= at the top the mask is not the short's values: expect sat")
(push 1)
(declare-const r (_ BitVec 32))
(assert (= r #x00007fff))
(assert (not (= (or (bvslt r #xffff8000) (bvsge r #x00007fff)) (or (bvslt r #xffff8000) (bvsgt r #x00007fff)))))
(check-sat)
(pop 1)

(echo "narrow_range: the native arm's multiply, in the 32-bit lane, is checked by the same mask: expect unsat")
(push 1)
(declare-const p (_ BitVec 32))
(assert (not (= (or (bvslt p #xffff8000) (bvsgt p #x00007fff)) (not (= p ((_ sign_extend 16) ((_ extract 15 0) p)))))))
(check-sat)
(pop 1)
