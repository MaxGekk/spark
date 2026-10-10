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

; The leap flag's perfect hash (VarkaChrono.isLeapYear, VarkaChronoLowering's leap flag) is
; the Gregorian rule over its whole domain (VARKA-242). Per lane, y the year biased by
; YEAR_BIAS, a multiple of 400, so leapness is unchanged and y is non-negative:
;
;   leap = Integer.compareUnsigned((y * LEAP_HASH_M) & LEAP_HASH_MASK, LEAP_HASH_MAX) <= 0
;                                   IntVector.mul, and, compare ULE
;
; Over 32-bit vectors, whose bvmul, bvand and bvule are Java's int *, & and the unsigned
; compare exactly: the hash is the rule (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) for
; every biased year up to LEAP_HASH_MAX_BIASED_YEAR, is not one year past it, so the bound
; is exact, and a signed compare would be wrong inside the range. Over integers this ran
; past ten minutes under Z3 (VARKA-240); over bit-vectors it takes under a second. The file
; declares ALL so that the prelude, which it does not use, can be inserted.
;
; Rendered by VarkaProofFiles. VarkaProofFilesSuite fails when this file differs from its
; rendering, and VARKA_PROOFS_REGEN=true build/sbt 'catalyst/testOnly *VarkaProofFilesSuite'
; rewrites it. Run by dev/varka_prove.sh, which inserts java.smt2 after the set-logic line.

(set-logic ALL)

(define-fun leap.hash ((y (_ BitVec 32))) (_ BitVec 32)
  (bvand (bvmul y #x400023d7) #xc001f00f))
(define-fun leap.gregorian ((y (_ BitVec 32))) Bool
  (and (= (bvurem y #x00000004) #x00000000)
       (or (not (= (bvurem y #x00000064) #x00000000))
           (= (bvurem y #x00000190) #x00000000))))

(echo "the hash, compared unsigned, is the Gregorian rule for every biased year to 102499: expect unsat")
(push 1)
(declare-const y (_ BitVec 32))
(assert (bvule y #x00019063))
(assert (not (= (bvule (leap.hash y) #x0001f000) (leap.gregorian y))))
(check-sat)
(pop 1)

(echo "and it is not at 102500: expect sat")
(push 1)
(declare-const y (_ BitVec 32))
(assert (= y #x00019064))
(assert (not (= (bvule (leap.hash y) #x0001f000) (leap.gregorian y))))
(check-sat)
(pop 1)

(echo "compared signed, it is not the rule inside the range: expect sat")
(push 1)
(declare-const y (_ BitVec 32))
(assert (bvule y #x00019063))
(assert (not (= (bvsle (leap.hash y) #x0001f000) (leap.gregorian y))))
(check-sat)
(pop 1)
