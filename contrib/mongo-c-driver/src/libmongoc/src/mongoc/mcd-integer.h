/**
 * Copyright 2022 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef MCD_INTEGER_H_INCLUDED
#define MCD_INTEGER_H_INCLUDED

#include "mongoc-prelude.h"

#include <bson/bson.h>

#include <stdint.h>
#include <stdbool.h>

/// Return 'true' iff (left * right) would overflow with int64
static BSON_INLINE bool
_mcd_i64_mul_would_overflow (int64_t left, int64_t right)
{
   if (right == -1) {
      // We will perform an integer division, and only (MIN / -1) is undefined
      // for integer division.
      return left == INT64_MIN;
   }

   if (right == 0) {
      // Multiplication by zero never overflows, and we cannot divide by zero
      return false;
   }

   // From here on, all integer division by 'right' is well-defined

   if (left > 0) {
      if (right > 0) {
         /**
         Given: left > 0
           and: right > 0
          then: left * right > 0
          THEN: left * right > MIN

         Define: max_fac         =  MAX / right
           then: max_fac * right = (MAX / right) * right
           then: max_fac * right =  MAX
         */
         const int64_t max_fac = INT64_MAX / right;
         if (left > max_fac) {
            /**
            Given: left         > max_fac
             then: left * right > max_fac * right
             with: MAX          = max_fac * right
             then: left * right > MAX
            */
            return true;
         } else {
            /**
            Given: left         <= max_fac
             then: left * right <= max_fac * right
             with: MAX          = max_fac * right
             THEN: left * right <= MAX
            */
            return false;
         }
      } else {
         /**
         Given: left > 0
           and: right <= 0
          then: left * right < 0
          THEN: left * right < MAX

         Define: min_fac        =  MIN / left
           then: min_Fac * left = (MIN / left) * left
           then: min_Fac * left =  MIN
         */
         const int64_t min_fac = INT64_MIN / left;
         if (right < min_fac) {
            /**
            Given: right          < min_fac
             then: right   * left < min_fac * left
             with: min_fac * left = MIN
             then: right   * left < MIN
            */
            return true;
         } else {
            /**
            Given: right          >= min_fac
             then: right   * left >= min_fac * left
             with: min_fac * left =  MIN
             then: right   * left >= MIN
            */
            return false;
         }
      }
   } else {
      if (right > 0) {
         /**
         Given: left <= 0
           and: right > 0
          then: left * right <= 0
          THEN: left * right <  MAX

         Define: min_fac         =  MIN / right
           then: min_fac * right = (MIN / right) * right
           then: min_fac * right =  MIN
         */
         const int64_t min_fac = INT64_MIN / right;
         if (left < min_fac) {
            /**
            Given: left         < min_fac
             then: left * right < min_fac * right
             with: MIN          = min_fac * right
             then: left * right < MIN
            */
            return true;
         } else {
            /**
            Given: left         >= min_fac
             then: left * right >= min_fac * right
             with: MIN          =  min_fac * right
             then: left * right >= MIN
            */
            return false;
         }
      } else {
         /**
         Given: left  <= 0
           and: right <= 0
          then: left * right >= 0
          THEN: left * right >  MIN
         */
         if (left == 0) {
            // Multiplication by zero will never overflow
            return false;
         } else {
            /**
            Given: left <= 0
              and: left != 0
             then: left <  0

            Define: max_fac        =  MAX / left
              then: max_fac * left = (MAX / left) * left
              then: max_fac * left =  MAX

            Given:   left < 0
              and:    MAX > 0
              and: max_fac = MAX / left
             then: max_fac < 0  [pos/neg -> neg]
            */
            const int64_t max_fac = INT64_MAX / left;
            if (right < max_fac) {
               /*
               Given:        right <  max_fac
                 and: left         <  0
                then: left * right >  max_fac     * left
                then: left * right > (MAX / left) * left
                then: left * right >  MAX
               */
               return true;
            } else {
               /*
               Given:        right >=  max_fac
                 and: left         <   0
                then: left * right <=  max_fac     * left
                then: left * right <= (MAX / left) * left
                then: left * right <=  MAX
               */
               return false;
            }
         }
      }
   }
}

/// Return 'true' iff (left + right) would overflow with int64
static BSON_INLINE bool
_mcd_i64_add_would_overflow (int64_t left, int64_t right)
{
   /**
    * Context:
    *
    * - MAX, MIN, left: right: ℤ
    * - left >= MIN
    * - left <= MAX
    * - right >= MIN
    * - right <= MAX
    * - forall (N, M, Q : ℤ) .
    *    if N = M then
    *       M = N  (Symmetry)
    *    N + 0 = N      (Zero is neutral)
    *    N + M = M + N  (Addition is commutative)
    *    if N < M then
    *       0 - N > 0 - M  (Order inversion)
    *           M >     N  (Symmetry inversion)
    *       0 - M < 0 - N  (order+symmetry inversion)
    *       if M < Q or M = Q then
    *          N < Q       (Order transitivity)
    *    0 - M = -M        (Negation is subtraction)
    *    N - M = N + (-M)
    *    Ord(N, M) = Ord(N+Q, M+Q) (Addition preserves ordering)
    */
   // MAX, MIN, left, right: ℤ
   //* Given: right <= MAX
   //* Given: left <= MAX

   if (right < 0) {
      /**
      Given:        right <         0
       then: left + right <  left + 0
       then: left + right <  left
       then: left + right <= left  [Weakening]

      Given: left <= MAX
        and: left + right <= left
       then: left + right <= MAX
       THEN: left + right CANNOT overflow MAX
      */

      /**
      Given:     right >=     MIN
       then: 0 - right <= 0 - MIN
       then:    -right <=    -MIN

      Given:       -right <=       -MIN
       then: MIN + -right <= MIN + -MIN
       then: MIN + -right <= 0
       then: MIN -  right <= 0
       then: MIN -  right <= MAX
       THEN: MIN - right CANNOT overflow MAX

      Given:     right <     0
       then: 0 - right > 0 - 0
       then: 0 - right > 0
       then:    -right > 0

      Given:        -right  >       0
       then: MIN + (-right) > MIN + 0
       then: MIN - right    > MIN + 0
       then: MIN - right    > MIN
       THEN: MIN - right CANNOT overflow MIN

      Define: legroom = MIN - right

      Given: legroom         = MIN - right
       then: legroom + right = MIN - right + right
       then: legroom + right = MIN
      */
      const int64_t legroom = INT64_MIN - right;

      if (left < legroom) {
         /**
         Given: left         < legroom
          then: left + right < legroom + right

         Given: legroom + right = MIN
           and: left + right < legroom + right
          then: left + right < MIN
          THEN: left + right WILL overflow MIN!
         */
         return true;
      } else {
         /**
         Given: left >= legroom
          then: left + right >= legroom + right

         Given: legroom + right = MIN
           and: left + right >= legroom + right
          THEN: left + right >= MIN

         Given: left + right <= MAX
           and: left + right >= MIN
          THEN: left + right is in [MIN, MAX]
         */
         return false;
      }
   } else if (right > 0) {
      /**
      Given:        right >         0
       then: left + right >  left + 0
       then: left + right >  left
       then: left + right >= left  [Weakening]

      Given: left >= MIN
        and: left + right >= left
       then: left + right >= MIN
       THEN: left + right cannot overflow MIN
      */

      /**
      Given:     right <=     MAX
       then: 0 - right >= 0 - MAX
       then:    -right >=    -MAX

      Given:       -right >=       -MAX
       then: MAX + -right >= MAX + -MAX
       then: MAX + -right >= 0
       then: MAX -  right >= 0
       then: MAX -  right >= MIN
       THEN: MAX - right CANNOT overflow MIN

      Given:         right  > 0
       then:   0 -   right  < 0 - 0
       then:        -right  < 0
       then: MAX + (-right) < MAX + 0
       then: MAX + (-right) < MAX
       then: MAX -   right  < MAX
       THEN: MAX - right CANNOT overflow MAX

      Define: headroom = MAX - right

      Given: headroom         = MAX - right;
       then: headroom + right = MAX - right + right
       then: headroom + right = MAX
      */
      int64_t headroom = INT64_MAX - right;

      if (left > headroom) {
         /**
         Given: left         > headroom
          then: left + right > headroom + right

         Given: left + right > headroom + right
           and: headroom + right = MAX
          then: left + right > MAX
          THEN: left + right WILL overflow MAX!
         */
         return true;
      } else {
         /**
         Given: left         <= headroom
          then: left + rigth <= headroom + right

         Given: left + right <= headroom + right
           and: headroom + right = MAX
          then: left + right <= MAX
          THEN: left + right CANNOT overflow MAX
         */
         return false;
      }
   } else {
      /**
      Given:        right =        0
        and: left + right = left + 0
       then: left + right = left

      Given: left <= MAX
        and: left >= MIN
        and: left + right = left
       then: left + right <= MAX
        and: left + right >= MIN
       THEN: left + right is in [MIN, MAX]
      */
      return false;
   }
}

/// Return 'true' iff (left - right) would overflow with int64
static BSON_INLINE bool
_mcd_i64_sub_would_overflow (int64_t left, int64_t right)
{
   // Lemma: N - M = N + (-M), therefore (N - M) is bounded iff (N + -M)
   // is bounded.
   if (right > 0) {
      return _mcd_i64_add_would_overflow (left, -right);
   } else if (right < 0) {
      if (left > 0) {
         return _mcd_i64_add_would_overflow (-left, right);
      } else {
         // Both negative. Subtracting two negatives will never overflow
         return false;
      }
   } else {
      // Given:        right =        0
      //  then: left - right = left - 0
      //  then: left - right = left
      //? THEN: left - right is bounded
      return false;
   }
}

#endif // MCD_INTEGER_H_INCLUDED
