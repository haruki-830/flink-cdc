/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.runtime.functions.impl;

import java.math.BigDecimal;

/** Comparison built-in functions. */
public class ComparisonFunctions {

    public static Boolean valueEquals(Object object1, Object object2) {
        if (object1 == null || object2 == null) {
            return null;
        }
        return object1.equals(object2);
    }

    public static boolean isDistinctFrom(Object object1, Object object2) {
        if (object1 == null || object2 == null) {
            return object1 != object2;
        }
        return !object1.equals(object2);
    }

    public static boolean isNotDistinctFrom(Object object1, Object object2) {
        return !isDistinctFrom(object1, object2);
    }

    private static int universalCompares(Object lhs, Object rhs) {
        Class<?> leftClass = lhs.getClass();
        Class<?> rightClass = rhs.getClass();
        if (leftClass.equals(rightClass) && lhs instanceof Comparable) {
            return ((Comparable) lhs).compareTo(rhs);
        } else if (lhs instanceof Number && rhs instanceof Number) {
            return Double.compare(((Number) lhs).doubleValue(), ((Number) rhs).doubleValue());
        } else {
            throw new RuntimeException(
                    "Comparison of unsupported data types: "
                            + leftClass.getName()
                            + " and "
                            + rightClass.getName());
        }
    }

    public static Boolean greaterThan(Object lhs, Object rhs) {
        if (lhs == null || rhs == null) {
            return null;
        }
        return universalCompares(lhs, rhs) > 0;
    }

    public static Boolean greaterThanOrEqual(Object lhs, Object rhs) {
        if (lhs == null || rhs == null) {
            return null;
        }
        return universalCompares(lhs, rhs) >= 0;
    }

    public static Boolean lessThan(Object lhs, Object rhs) {
        if (lhs == null || rhs == null) {
            return null;
        }
        return universalCompares(lhs, rhs) < 0;
    }

    public static Boolean lessThanOrEqual(Object lhs, Object rhs) {
        if (lhs == null || rhs == null) {
            return null;
        }
        return universalCompares(lhs, rhs) <= 0;
    }

    public static Boolean betweenAsymmetric(String value, String minValue, String maxValue) {
        return between(value, minValue, maxValue);
    }

    public static Boolean betweenAsymmetric(Byte value, Byte minValue, Byte maxValue) {
        return between(value, minValue, maxValue);
    }

    public static Boolean betweenAsymmetric(Short value, Short minValue, Short maxValue) {
        return between(value, minValue, maxValue);
    }

    public static Boolean betweenAsymmetric(Integer value, Integer minValue, Integer maxValue) {
        return between(value, minValue, maxValue);
    }

    public static Boolean betweenAsymmetric(Long value, Long minValue, Long maxValue) {
        return between(value, minValue, maxValue);
    }

    public static Boolean betweenAsymmetric(Float value, Float minValue, Float maxValue) {
        return between(value, minValue, maxValue);
    }

    public static Boolean betweenAsymmetric(Double value, Double minValue, Double maxValue) {
        return between(value, minValue, maxValue);
    }

    public static Boolean betweenAsymmetric(
            BigDecimal value, BigDecimal minValue, BigDecimal maxValue) {
        return between(value, minValue, maxValue);
    }

    private static Boolean between(Object value, Object minValue, Object maxValue) {
        Boolean lowerResult = greaterThanOrEqual(value, minValue);
        Boolean upperResult = lessThanOrEqual(value, maxValue);
        return LogicalFunctions.and(lowerResult, upperResult);
    }

    public static Boolean notBetweenAsymmetric(String value, String minValue, String maxValue) {
        return LogicalFunctions.not(betweenAsymmetric(value, minValue, maxValue));
    }

    public static Boolean notBetweenAsymmetric(Byte value, Byte minValue, Byte maxValue) {
        return LogicalFunctions.not(betweenAsymmetric(value, minValue, maxValue));
    }

    public static Boolean notBetweenAsymmetric(Short value, Short minValue, Short maxValue) {
        return LogicalFunctions.not(betweenAsymmetric(value, minValue, maxValue));
    }

    public static Boolean notBetweenAsymmetric(Integer value, Integer minValue, Integer maxValue) {
        return LogicalFunctions.not(betweenAsymmetric(value, minValue, maxValue));
    }

    public static Boolean notBetweenAsymmetric(Long value, Long minValue, Long maxValue) {
        return LogicalFunctions.not(betweenAsymmetric(value, minValue, maxValue));
    }

    public static Boolean notBetweenAsymmetric(Float value, Float minValue, Float maxValue) {
        return LogicalFunctions.not(betweenAsymmetric(value, minValue, maxValue));
    }

    public static Boolean notBetweenAsymmetric(Double value, Double minValue, Double maxValue) {
        return LogicalFunctions.not(betweenAsymmetric(value, minValue, maxValue));
    }

    public static Boolean notBetweenAsymmetric(
            BigDecimal value, BigDecimal minValue, BigDecimal maxValue) {
        return LogicalFunctions.not(betweenAsymmetric(value, minValue, maxValue));
    }

    public static Boolean in(String value, String... values) {
        return inValues(value, values);
    }

    public static Boolean in(Byte value, Byte... values) {
        return inValues(value, values);
    }

    public static Boolean in(Short value, Short... values) {
        return inValues(value, values);
    }

    public static Boolean in(Integer value, Integer... values) {
        return inValues(value, values);
    }

    public static Boolean in(Long value, Long... values) {
        return inValues(value, values);
    }

    public static Boolean in(Float value, Float... values) {
        return inValues(value, values);
    }

    public static Boolean in(Double value, Double... values) {
        return inValues(value, values);
    }

    public static Boolean in(BigDecimal value, BigDecimal... values) {
        return inValues(value, values);
    }

    private static Boolean inValues(Object value, Object... values) {
        boolean hasUnknown = false;
        for (Object candidate : values) {
            Boolean equals = valueEquals(value, candidate);
            if (Boolean.TRUE.equals(equals)) {
                return true;
            }
            if (equals == null) {
                hasUnknown = true;
            }
        }
        return hasUnknown ? null : false;
    }

    public static Boolean notIn(String value, String... values) {
        return LogicalFunctions.not(in(value, values));
    }

    public static Boolean notIn(Byte value, Byte... values) {
        return LogicalFunctions.not(in(value, values));
    }

    public static Boolean notIn(Short value, Short... values) {
        return LogicalFunctions.not(in(value, values));
    }

    public static Boolean notIn(Integer value, Integer... values) {
        return LogicalFunctions.not(in(value, values));
    }

    public static Boolean notIn(Long value, Long... values) {
        return LogicalFunctions.not(in(value, values));
    }

    public static Boolean notIn(Float value, Float... values) {
        return LogicalFunctions.not(in(value, values));
    }

    public static Boolean notIn(Double value, Double... values) {
        return LogicalFunctions.not(in(value, values));
    }

    public static Boolean notIn(BigDecimal value, BigDecimal... values) {
        return LogicalFunctions.not(in(value, values));
    }
}
