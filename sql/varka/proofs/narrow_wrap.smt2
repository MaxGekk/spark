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

; The narrow lanes' wrap (VARKA-330). A byte or short result is signExtend(r mod 2^w) of
; the exact result r, which is what Spark's ByteExactNumeric and ShortExactNumeric
; answer (VARKA-329.md 4.1). The widened arm computes at the int lane and reduces here
; where a consumer that is not a ring operation reads the value:
;
;   wrap(x) = (x << s) >>a s,   s = 32 - w        VarkaVectorWalk.emitWrapToNarrow
;
; w is the lane's width, 8 or 16, and s is read from the lane here. The first claim is
; that the pair is the sign extension of the low w bits for every int; the next three
; that the ring operations commute with it, so reducing once at the end of a chain of
; adds, subtracts and multiplies agrees with reducing at every node. Beside them, that
; the domain admits the lane's two ends, and the neighbours: the logical shift and the
; shift one less are not the wrap.
;
; The logic is ALL because the file reasons over bit-vectors.
;
; Rendered by VarkaProofFiles. VarkaProofFilesSuite fails when this file differs from its
; rendering, and VARKA_PROOFS_REGEN=true build/sbt 'catalyst/testOnly *VarkaProofFilesSuite'
; rewrites it. Run by dev/varka_prove.sh, which inserts java.smt2 after the set-logic line.

(set-logic ALL)

(echo "narrow_wrap: the shift pair is sign extension of the low 8 bits for every int: expect unsat")
(push 1)
(declare-const x (_ BitVec 32))
(assert (not (= (bvashr (bvshl x #x00000018) #x00000018) ((_ sign_extend 24) ((_ extract 7 0) x)))))
(check-sat)
(pop 1)

(echo "narrow_wrap: the int lane's add, reduced, is the byte's add: expect unsat")
(push 1)
(declare-const a (_ BitVec 8))
(declare-const b (_ BitVec 8))
(assert (not (= (bvashr (bvshl (bvadd ((_ sign_extend 24) a) ((_ sign_extend 24) b)) #x00000018) #x00000018) ((_ sign_extend 24) ((_ extract 7 0) (bvadd a b))))))
(check-sat)
(pop 1)

(echo "narrow_wrap: the int lane's sub, reduced, is the byte's sub: expect unsat")
(push 1)
(declare-const a (_ BitVec 8))
(declare-const b (_ BitVec 8))
(assert (not (= (bvashr (bvshl (bvsub ((_ sign_extend 24) a) ((_ sign_extend 24) b)) #x00000018) #x00000018) ((_ sign_extend 24) ((_ extract 7 0) (bvsub a b))))))
(check-sat)
(pop 1)

(echo "narrow_wrap: the int lane's mul, reduced, is the byte's mul: expect unsat")
(push 1)
(declare-const a (_ BitVec 8))
(declare-const b (_ BitVec 8))
(assert (not (= (bvashr (bvshl (bvmul ((_ sign_extend 24) a) ((_ sign_extend 24) b)) #x00000018) #x00000018) ((_ sign_extend 24) ((_ extract 7 0) (bvmul a b))))))
(check-sat)
(pop 1)

(echo "narrow_wrap: the domain admits both ends of the byte, whose sum wraps to -1: expect sat")
(push 1)
(declare-const a (_ BitVec 8))
(declare-const b (_ BitVec 8))
(assert (= a #x80))
(assert (= b #x7f))
(assert (= (bvadd a b) #xff))
(check-sat)
(pop 1)

(echo "narrow_wrap: with the logical shift it is not: expect sat")
(push 1)
(declare-const x (_ BitVec 32))
(assert (not (= (bvlshr (bvshl x #x00000018) #x00000018) ((_ sign_extend 24) ((_ extract 7 0) x)))))
(check-sat)
(pop 1)

(echo "narrow_wrap: with the shift one less it is not: expect sat")
(push 1)
(declare-const x (_ BitVec 32))
(assert (not (= (bvashr (bvshl x #x00000017) #x00000017) ((_ sign_extend 24) ((_ extract 7 0) x)))))
(check-sat)
(pop 1)

(echo "narrow_wrap: with the shift one more it is not: expect sat")
(push 1)
(declare-const x (_ BitVec 32))
(assert (not (= (bvashr (bvshl x #x00000019) #x00000019) ((_ sign_extend 24) ((_ extract 7 0) x)))))
(check-sat)
(pop 1)

(echo "narrow_wrap: the shift pair is sign extension of the low 16 bits for every int: expect unsat")
(push 1)
(declare-const x (_ BitVec 32))
(assert (not (= (bvashr (bvshl x #x00000010) #x00000010) ((_ sign_extend 16) ((_ extract 15 0) x)))))
(check-sat)
(pop 1)

(echo "narrow_wrap: the int lane's add, reduced, is the short's add: expect unsat")
(push 1)
(declare-const a (_ BitVec 16))
(declare-const b (_ BitVec 16))
(assert (not (= (bvashr (bvshl (bvadd ((_ sign_extend 16) a) ((_ sign_extend 16) b)) #x00000010) #x00000010) ((_ sign_extend 16) ((_ extract 15 0) (bvadd a b))))))
(check-sat)
(pop 1)

(echo "narrow_wrap: the int lane's sub, reduced, is the short's sub: expect unsat")
(push 1)
(declare-const a (_ BitVec 16))
(declare-const b (_ BitVec 16))
(assert (not (= (bvashr (bvshl (bvsub ((_ sign_extend 16) a) ((_ sign_extend 16) b)) #x00000010) #x00000010) ((_ sign_extend 16) ((_ extract 15 0) (bvsub a b))))))
(check-sat)
(pop 1)

(echo "narrow_wrap: the int lane's mul, reduced, is the short's mul: expect unsat")
(push 1)
(declare-const a (_ BitVec 16))
(declare-const b (_ BitVec 16))
(assert (not (= (bvashr (bvshl (bvmul ((_ sign_extend 16) a) ((_ sign_extend 16) b)) #x00000010) #x00000010) ((_ sign_extend 16) ((_ extract 15 0) (bvmul a b))))))
(check-sat)
(pop 1)

(echo "narrow_wrap: the domain admits both ends of the short, whose sum wraps to -1: expect sat")
(push 1)
(declare-const a (_ BitVec 16))
(declare-const b (_ BitVec 16))
(assert (= a #x8000))
(assert (= b #x7fff))
(assert (= (bvadd a b) #xffff))
(check-sat)
(pop 1)

(echo "narrow_wrap: with the logical shift it is not: expect sat")
(push 1)
(declare-const x (_ BitVec 32))
(assert (not (= (bvlshr (bvshl x #x00000010) #x00000010) ((_ sign_extend 16) ((_ extract 15 0) x)))))
(check-sat)
(pop 1)

(echo "narrow_wrap: with the shift one less it is not: expect sat")
(push 1)
(declare-const x (_ BitVec 32))
(assert (not (= (bvashr (bvshl x #x0000000f) #x0000000f) ((_ sign_extend 16) ((_ extract 15 0) x)))))
(check-sat)
(pop 1)

(echo "narrow_wrap: with the shift one more it is not: expect sat")
(push 1)
(declare-const x (_ BitVec 32))
(assert (not (= (bvashr (bvshl x #x00000011) #x00000011) ((_ sign_extend 16) ((_ extract 15 0) x)))))
(check-sat)
(pop 1)
