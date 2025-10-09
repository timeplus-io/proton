---
toc_priority: 34
toc_title: Arithmetic
---

# Arithmetic Functions {#arithmetic-functions}

For all arithmetic functions, the result type is calculated as the smallest number type that the result fits in, if there is such a type. The minimum is taken simultaneously based on the number of bits, whether it is signed, and whether it floats. If there are not enough bits, the highest bit type is taken.

Example:

``` sql
SELECT toTypeName(0), toTypeName(0 + 0), toTypeName(0 + 0 + 0), toTypeName(0 + 0 + 0 + 0)
```

``` text
┌─toTypeName(0)─┬─toTypeName(plus(0, 0))─┬─toTypeName(plus(plus(0, 0), 0))─┬─toTypeName(plus(plus(plus(0, 0), 0), 0))─┐
│ UInt8         │ UInt16                 │ UInt32                          │ UInt64                                   │
└───────────────┴────────────────────────┴─────────────────────────────────┴──────────────────────────────────────────┘
```

Arithmetic functions work for any pair of types from UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, or Float64.

Overflow is produced the same way as in C++.

## plus(a, b), a + b operator {#plusa-b-a-b-operator}

Calculates the sum of the numbers.
You can also add integer numbers with a date or date and time. In the case of a date, adding an integer means adding the corresponding number of days. For a date with time, it means adding the corresponding number of seconds.

## minus(a, b), a - b operator {#minusa-b-a-b-operator}

Calculates the difference. The result is always signed.

You can also calculate integer numbers from a date or date with time. The idea is the same – see above for ‘plus’.

## multiply(a, b), a \* b operator {#multiplya-b-a-b-operator}

Calculates the product of the numbers.

## divide(a, b), a / b operator {#dividea-b-a-b-operator}

Calculates the quotient of the numbers. The result type is always a floating-point type.
It is not integer division. For integer division, use the ‘intDiv’ function.
When dividing by zero you get ‘inf’, ‘-inf’, or ‘nan’.

## intDiv(a, b) {#intdiva-b}

Calculates the quotient of the numbers. Divides into integers, rounding down (by the absolute value).
An exception is thrown when dividing by zero or when dividing a minimal negative number by minus one.

## intDivOrZero(a, b) {#intdivorzeroa-b}

Differs from ‘intDiv’ in that it returns zero when dividing by zero or when dividing a minimal negative number by minus one.

## modulo(a, b), a % b operator {#modulo}

Calculates the remainder after division.
If arguments are floating-point numbers, they are pre-converted to integers by dropping the decimal portion.
The remainder is taken in the same sense as in C++. Truncated division is used for negative numbers.
An exception is thrown when dividing by zero or when dividing a minimal negative number by minus one.

## moduloOrZero(a, b) {#modulo-or-zero}

Differs from [modulo](#modulo) in that it returns zero when the divisor is zero.

## negate(a), -a operator {#negatea-a-operator}

Calculates a number with the reverse sign. The result is always signed.

## abs(a) {#arithm_func-abs}

Calculates the absolute value of the number (a). That is, if a \< 0, it returns -a. For unsigned types it does not do anything. For signed integer types, it returns an unsigned number.

## gcd(a, b) {#gcda-b}

Returns the greatest common divisor of the numbers.
An exception is thrown when dividing by zero or when dividing a minimal negative number by minus one.

## lcm(a, b) {#lcma-b}

Returns the least common multiple of the numbers.
An exception is thrown when dividing by zero or when dividing a minimal negative number by minus one.

## max2 {#max2}

Compares two values and returns the maximum. The returned value is converted to [Float64](../../sql-reference/data-types/float.md).

**Syntax**

```sql
max2(value1, value2)
plus(a, b)
```

It is possible to add an integer and a date or date with time. The former operation increments the number of days in the date, the latter operation increments the number of seconds in the date with time.

Alias: `a + b` (operator)

## minus {#minus}

Calculates the difference of two values `a` and `b`. The result is always signed.

Similar to `plus`, it is possible to subtract an integer from a date or date with time.

Additionally, subtraction between date with time is supported, resulting in the time difference between them.

**Syntax**

```sql
minus(a, b)
```

Alias: `a - b` (operator)

## multiply {#multiply}

Calculates the product of two values `a` and `b`.

**Syntax**

```sql
multiply(a, b)
```

Alias: `a * b` (operator)

## divide {#divide}

Calculates the quotient of two values `a` and `b`. The result type is always [Float64](../data-types/float.md). Integer division is provided by the `intDiv` function.

Division by 0 returns `inf`, `-inf`, or `nan`.

**Syntax**

```sql
divide(a, b)
```

Alias: `a / b` (operator)

## divideOrNull {#divideornull}

Like [divide](#divide) but returns null when the divisor is zero.

**Syntax**

```sql
divideOrNull(a, b)
```

## intDiv {#intdiv}

Performs an integer division of two values `a` by `b`, i.e. computes the quotient rounded down to the next smallest integer.

The result has the same width as the dividend (the first parameter).

An exception is thrown when dividing by zero, when the quotient does not fit in the range of the dividend, or when dividing a minimal negative number by minus one.

**Syntax**

```sql
intDiv(a, b)
```

**Example**

Query:

```sql
SELECT
    intDiv(toFloat64(1), 0.001) AS res,
    toTypeName(res)
```

```response
┌──res─┬─toTypeName(intDiv(toFloat64(1), 0.001))─┐
│ 1000 │ Int64                                   │
└──────┴─────────────────────────────────────────┘
```

```sql
SELECT
    intDiv(1, 0.001) AS res,
    toTypeName(res)
```

```response
Received exception from server (version 23.2.1):
Code: 153. DB::Exception: Received from localhost:9000. DB::Exception: Cannot perform integer division, because it will produce infinite or too large number: While processing intDiv(1, 0.001) AS res, toTypeName(res). (ILLEGAL_DIVISION)
```

## intDivOrZero {#intdivorzero}

Same as `intDiv` but returns zero when dividing by zero or when dividing a minimal negative number by minus one.

**Syntax**

```sql
intDivOrZero(a, b)
```

## intDivOrNull {#intdivornull}

Like [intDiv](#intdiv) but returns null when the divisor is zero.

**Syntax**

```sql
intDivOrNull(a, b)
```

## isFinite {#isfinite}

Returns 1 if the Float32 or Float64 argument not infinite and not a NaN, otherwise this function returns 0.

**Syntax**

```sql
isFinite(x)
```

## isInfinite {#isinfinite}

Returns 1 if the Float32 or Float64 argument is infinite, otherwise this function returns 0. Note that 0 is returned for a NaN.

**Syntax**

```sql
isInfinite(x)
```

## ifNotFinite {#ifnotfinite}

Checks whether a floating point value is finite.

**Syntax**

```sql
ifNotFinite(x,y)
```

**Arguments**

-   `value1` — First value. [Int/UInt](../../sql-reference/data-types/int-uint.md) or [Float](../../sql-reference/data-types/float.md).
-   `value2` — Second value. [Int/UInt](../../sql-reference/data-types/int-uint.md) or [Float](../../sql-reference/data-types/float.md).

**Returned value**

-   The maximum of two values.

Type: [Float](../../sql-reference/data-types/float.md).
**Example**

Query:

    SELECT 1/0 as infimum, ifNotFinite(infimum,42)

Result:

    ┌─infimum─┬─ifNotFinite(divide(1, 0), 42)─┐
    │     inf │                            42 │
    └─────────┴───────────────────────────────┘

You can get similar result by using the [ternary operator](/sql-reference/functions/conditional-functions#if): `isFinite(x) ? x : y`.

## isNaN {#isnan}

Returns 1 if the Float32 and Float64 argument is NaN, otherwise this function 0.

**Syntax**

```sql
isNaN(x)
```

## modulo {#modulo}

Calculates the remainder of the division of two values `a` by `b`.

The result type is an integer if both inputs are integers. If one of the inputs is a floating-point number, the result type is [Float64](../data-types/float.md).

The remainder is computed like in C++. Truncated division is used for negative numbers.

An exception is thrown when dividing by zero or when dividing a minimal negative number by minus one.

**Syntax**

```sql
modulo(a, b)
```

Alias: `a % b` (operator)

## moduloOrZero {#moduloorzero}

Like [modulo](#modulo) but returns zero when the divisor is zero.

**Syntax**

```sql
moduloOrZero(a, b)
```

## moduloOrNull {#moduloornull}

Like [modulo](#modulo) but returns null when the divisor is zero.

**Syntax**

```sql
moduloOrNull(a, b)
```

## positiveModulo(a, b) {#positivemoduloa-b}

Like [modulo](#modulo) but always returns a non-negative number.

This function is 4-5 times slower than `modulo`.

**Syntax**

```sql
positiveModulo(a, b)
```

Alias:
- `positive_modulo(a, b)`
- `pmod(a, b)`

**Example**

Query:

```sql
SELECT positiveModulo(-1, 10)
```

Result:

```result
┌─positiveModulo(-1, 10)─┐
│                      9 │
└────────────────────────┘
```

## positiveModuloOrNull(a, b) {#positivemoduloornulla-b}

Like [positiveModulo](#positivemoduloa-b) but returns null when the divisor is zero.

**Syntax**

```sql
positiveModuloOrNull(a, b)
```

## negate {#negate}

Negates a value `a`. The result is always signed.

**Syntax**

```sql
negate(a)
```

Alias: `-a`

## abs {#abs}

Calculates the absolute value of `a`. Has no effect if `a` is of an unsigned type. If `a` is of a signed type, it returns an unsigned number.

**Syntax**

```sql
abs(a)
```

## gcd {#gcd}

Returns the greatest common divisor of two values `a` and `b`.

An exception is thrown when dividing by zero or when dividing a minimal negative number by minus one.

**Syntax**

```sql
gcd(a, b)
```

## lcm(a, b) {#lcma-b}

Returns the least common multiple of two values `a` and `b`.

An exception is thrown when dividing by zero or when dividing a minimal negative number by minus one.

**Syntax**

```sql
lcm(a, b)
```

## max2 {#max2}

Returns the bigger of two values `a` and `b`. The returned value is of type [Float64](../data-types/float.md).

**Syntax**

```sql
max2(a, b)
```

**Example**

Query:

```sql
SELECT max2(-1, 2);
```

Result:

```text
┌─max2(-1, 2)─┐
│           2 │
└─────────────┘
```

## min2 {#min2}

Compares two values and returns the minimum. The returned value is converted to [Float64](../../sql-reference/data-types/float.md).

**Syntax**

```sql
max2(value1, value2)
```

**Arguments**

-   `value1` — First value. [Int/UInt](../../sql-reference/data-types/int-uint.md) or [Float](../../sql-reference/data-types/float.md).
-   `value2` — Second value. [Int/UInt](../../sql-reference/data-types/int-uint.md) or [Float](../../sql-reference/data-types/float.md).

**Returned value**

-   The minimum of two values.

Type: [Float](../../sql-reference/data-types/float.md).

**Example**

Query:

```sql
SELECT min2(-1, 2);
```

Result:

```text
┌─min2(-1, 2)─┐
│          -1 │
└─────────────┘
```
