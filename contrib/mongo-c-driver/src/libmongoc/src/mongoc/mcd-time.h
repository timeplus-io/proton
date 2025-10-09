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

#ifndef MCD_TIME_H_INCLUDED
#define MCD_TIME_H_INCLUDED

#include "mongoc-prelude.h"

#include "./mcd-integer.h"

#include <bson/bson.h>


/**
 * @brief Represents an abstract point-in-time.
 *
 * @note This is an *abstract* time point, with the only guarantee that it
 * is strictly ordered with every other time point and that the difference
 * between any two times will roughly encode actual wall-clock durations.
 */
typedef struct mcd_time_point {
   /// The internal representation of the time.
   int64_t _rep;
} mcd_time_point;

/// The latest representable future point-in-time
#define MCD_TIME_POINT_MAX ((mcd_time_point){._rep = INT64_MAX})
/// The oldest representable past point-in-time
#define MCD_TIME_POINT_MIN ((mcd_time_point){._rep = INT64_MIN})

/**
 * @brief Represents a (possibly negative) duration of time.
 *
 * Construct this using one of the duration constructor functions.
 *
 * @note This encodes real wall-time durations, and may include negative
 * durations. It can be compared with other durations and used to offset
 * time points.
 */
typedef struct mcd_duration {
   /// An internal representation of the duration
   int64_t _rep;
} mcd_duration;

/// The maximum representable duration
#define MCD_DURATION_MAX ((mcd_duration){._rep = INT64_MAX})
/// The minimal representable (negative) duration
#define MCD_DURATION_MIN ((mcd_duration){._rep = INT64_MIN})
/// A duration representing zero amount of time
#define MCD_DURATION_ZERO ((mcd_duration){._rep = 0})

/**
 * @brief Obtain the current time point. This is only an abstract
 * monotonically increasing time, and does not necessarily correlate with
 * any real-world clock.
 */
static BSON_INLINE mcd_time_point
mcd_now (void)
{
   // Create a time point representing the current time.
   return (mcd_time_point){._rep = bson_get_monotonic_time ()};
}

/**
 * @brief Create a duration from a number of microseconds.
 *
 * @param s A number of microseconds
 * @return mcd_duration A duration corresponding to 's' microseconds.
 *
 * @note Saturates to the min/max duration if the duration is too great in
 * magnitude.
 */
static BSON_INLINE mcd_duration
mcd_microseconds (int64_t s)
{
   // 'mcd_duration' is encoded in a number of microseconds, so we don't need to
   // do bounds checking here.
   return (mcd_duration){._rep = s};
}

/**
 * @brief Create a duration from a number of milliseconds.
 *
 * @param s A number of milliseconds
 * @return mcd_duration A duration corresponding to 's' milliseconds.
 *
 * @note Saturates to the min/max duration if the duration is too great in
 * magnitude.
 */
static BSON_INLINE mcd_duration
mcd_milliseconds (int64_t s)
{
   // 1'000 microseconds per millisecond:
   if (_mcd_i64_mul_would_overflow (s, 1000)) {
      return s < 0 ? MCD_DURATION_MIN : MCD_DURATION_MAX;
   }
   return mcd_microseconds (s * 1000);
}

/**
 * @brief Create a duration from a number of seconds.
 *
 * @param s A number of seconds
 * @return mcd_duration A duration corresponding to 's' seconds.
 *
 * @note Saturates to the min/max duration if the duration is too great in
 * magnitude.
 */
static BSON_INLINE mcd_duration
mcd_seconds (int64_t s)
{
   // 1'000 milliseconds per second:
   if (_mcd_i64_mul_would_overflow (s, 1000)) {
      return s < 0 ? MCD_DURATION_MIN : MCD_DURATION_MAX;
   }
   return mcd_milliseconds (s * 1000);
}

/**
 * @brief Create a duration from a number of minutes.
 *
 * @param m A number of minutes
 * @return mcd_duration A duration corresponding to 's' minutes.
 *
 * @note Saturates to the min/max duration if the duration is too great in
 * magnitude.
 */
static BSON_INLINE mcd_duration
mcd_minutes (int64_t m)
{
   // Sixty seconds per minute:
   if (_mcd_i64_mul_would_overflow (m, 60)) {
      return m < 0 ? MCD_DURATION_MIN : MCD_DURATION_MAX;
   }
   return mcd_seconds (m * 60);
}

/**
 * @brief Obtain the count of full milliseconds encoded in the given duration
 *
 * @param d An abstract duration
 * @return int64_t The number of milliseconds in 'd'
 *
 * @note Does not round-trip with `mcd_milliseconds(N)` if N-milliseconds is
 * unrepresentable in the duration type. This only occurs in extreme durations
 */
static BSON_INLINE int64_t
mcd_get_milliseconds (mcd_duration d)
{
   return d._rep / 1000;
}

/**
 * @brief Obtain a point-in-time relative to a base time offset by the given
 * duration (which may be negative).
 *
 * @param from The basis of the time offset
 * @param delta The amount of time to shift the resulting time point
 * @return mcd_time_point If 'delta' is a positive duration, the result is a
 * point-in-time *after* 'from'. If 'delta' is a negative duration, the result
 * is a point-in-time *before* 'from'.
 *
 * @note If the resulting point-in-time is unrepresentable, the return value
 * will be clamped to MCD_TIME_POINT_MIN or MCD_TIME_POINT_MAX.
 */
static BSON_INLINE mcd_time_point
mcd_later (mcd_time_point from, mcd_duration delta)
{
   if (_mcd_i64_add_would_overflow (from._rep, delta._rep)) {
      return delta._rep < 0 ? MCD_TIME_POINT_MIN : MCD_TIME_POINT_MAX;
   } else {
      from._rep += delta._rep;
      return from;
   }
}

/**
 * @brief Obtain the duration between two points in time.
 *
 * @param then The target time
 * @param from The base time
 * @return mcd_duration The amount of time you would need to wait starting
 * at 'from' for the time to become 'then' (the result may be a negative
 * duration).
 *
 * Intuition: If "then" is "in the future" relative to "from", you will
 * receive a positive duration, indicating an amount of time to wait
 * beginning at 'from' to reach 'then'. If "then" is actually *before*
 * "from", you will receive a paradoxical *negative* duration, indicating
 * the amount of time needed to time-travel backwards to reach "then."
 */
static BSON_INLINE mcd_duration
mcd_time_difference (mcd_time_point then, mcd_time_point from)
{
   if (_mcd_i64_sub_would_overflow (then._rep, from._rep)) {
      if (from._rep < 0) {
         // Would overflow past the max
         return MCD_DURATION_MAX;
      } else {
         // Would overflow past the min
         return MCD_DURATION_MIN;
      }
   } else {
      int64_t diff = then._rep - from._rep;
      // Our time_point encodes the time using a microsecond counter.
      return mcd_microseconds (diff);
   }
}

/**
 * @brief Compare two time points to create an ordering.
 *
 * A time point "in the past" is "less than" a time point "in the future".
 *
 * @retval <0 If 'left' is before 'right'
 * @retval >0 If 'right' is before 'left'
 * @retval  0 If 'left' and 'right' are equivalent
 */
static BSON_INLINE int
mcd_time_compare (mcd_time_point left, mcd_time_point right)
{
   // Obtain the amount of time needed to wait from 'right' to reach
   // 'left'
   int64_t diff = mcd_time_difference (left, right)._rep;
   if (diff < 0) {
      // A negative duration indicates that 'left' is "before" 'right'
      return -1;
   } else if (diff > 0) {
      // A positive duration indicates that 'left' is "after" 'right'
      return 1;
   } else {
      // These time points are equivalent
      return 0;
   }
}

/**
 * @brief Compare two durations
 *
 * A duration D1 is "less than" a duration D2 if time-travelling/waiting for D1
 * duration would end in the past relative to time-travelling/waiting for D2.
 *
 * @retval <0 If left is "less than" right
 * @retval >0 If left is "greater than" right
 * @retval  0 If left and right are equivalent
 */
static BSON_INLINE int
mcd_duration_compare (mcd_duration left, mcd_duration right)
{
   if (left._rep < right._rep) {
      return -1;
   } else if (left._rep > right._rep) {
      return 1;
   } else {
      return 0;
   }
}

/**
 * @brief Clamp a duration between two other durations
 *
 * @param dur The duration to transform
 * @param min The minimum duration
 * @param max The maximum duration
 * @retval min If `dur` < `min`
 * @retval max If `dur` > `max`
 * @retval dur Otherwise
 */
static BSON_INLINE mcd_duration
mcd_duration_clamp (mcd_duration dur, mcd_duration min, mcd_duration max)
{
   BSON_ASSERT (mcd_duration_compare (min, max) <= 0 && "Invalid min-max range given to mcd_duration_clamp()");
   if (mcd_duration_compare (dur, min) < 0) {
      // The duration is less than the minimum
      return min;
   } else if (mcd_duration_compare (dur, max) > 0) {
      // The duration is greater than the maximum
      return max;
   } else {
      // The duration is in-bounds
      return dur;
   }
}

/// Represents a timer that can be expired
typedef struct mcd_timer {
   /// The point in time after which the time will become expired.
   mcd_time_point expire_at;
} mcd_timer;

/// Create a time that will expire at the given time
static BSON_INLINE mcd_timer
mcd_timer_expire_at (mcd_time_point time)
{
   return (mcd_timer){time};
}

/**
 * @brief Create a timer that will expire after waiting for the given duration
 * relative to now
 *
 * @note If the duration is less-than or equal-to zero, the timer will already
 * have expired
 */
static BSON_INLINE mcd_timer
mcd_timer_expire_after (mcd_duration after)
{
   return mcd_timer_expire_at (mcd_later (mcd_now (), after));
}

/**
 * @brief Obtain the amount of time that one will need to WAIT before the timer
 * will be an expired state.
 *
 * @return mcd_duration A non-negative duration.
 *
 * @note If the timer is already expired, returns a zero duration. Will never
 * return a negative duration.
 */
static BSON_INLINE mcd_duration
mcd_timer_remaining (mcd_timer timer)
{
   // Compute the distance until the expiry time relative to now
   mcd_duration remain = mcd_time_difference (timer.expire_at, mcd_now ());
   // Compare that duration with a zero duration
   if (mcd_duration_compare (remain, mcd_microseconds (0)) < 0) {
      // The "remaining" time is less-than zero, which means the timer is
      // already expired, so we only need to wait for zero time:
      return mcd_microseconds (0);
   }
   // There is a positive amount of time remaining
   return remain;
}

#endif // MCD_TIME_H_INCLUDED
