/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.filter;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.druid.segment.DimensionHandlerUtils;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A {@link DruidPredicateFactory} that checks if input values equal a specific, provided value. Initialization work
 * is lazy and thread-safe.
 */
public class SelectorPredicateFactory implements DruidPredicateFactory
{
  @Nullable
  private final String value;

  private final Object initLock = new Object();

  private volatile DruidLongPredicate longPredicate;
  private volatile DruidFloatPredicate floatPredicate;
  private volatile DruidDoublePredicate doublePredicate;
  private final boolean isNullUnknown;

  public SelectorPredicateFactory(@Nullable String value)
  {
    this.value = value;
    this.isNullUnknown = value != null;
  }

  @Override
  public Predicate<String> makeStringPredicate()
  {
    return Predicates.equalTo(value);
  }

  @Override
  public DruidLongPredicate makeLongPredicate()
  {
    initLongPredicate();
    return longPredicate;
  }

  @Override
  public DruidFloatPredicate makeFloatPredicate()
  {
    initFloatPredicate();
    return floatPredicate;
  }

  @Override
  public DruidDoublePredicate makeDoublePredicate()
  {
    initDoublePredicate();
    return doublePredicate;
  }

  @Override
  public boolean isNullInputUnknown()
  {
    return isNullUnknown;
  }

  private void initLongPredicate()
  {
    if (longPredicate != null) {
      return;
    }
    synchronized (initLock) {
      if (longPredicate != null) {
        return;
      }
      if (value == null) {
        longPredicate = DruidLongPredicate.MATCH_NULL_ONLY;
        return;
      }
      final Long valueAsLong = DimensionHandlerUtils.convertObjectToLong(value);

      if (valueAsLong == null) {
        longPredicate = DruidLongPredicate.ALWAYS_FALSE;
      } else {
        // store the primitive, so we don't unbox for every comparison
        final long unboxedLong = valueAsLong;
        longPredicate = input -> input == unboxedLong;
      }
    }
  }

  private void initFloatPredicate()
  {
    if (floatPredicate != null) {
      return;
    }
    synchronized (initLock) {
      if (floatPredicate != null) {
        return;
      }

      if (value == null) {
        floatPredicate = DruidFloatPredicate.MATCH_NULL_ONLY;
        return;
      }
      final Float valueAsFloat = DimensionHandlerUtils.convertObjectToFloat(value);

      if (valueAsFloat == null) {
        floatPredicate = DruidFloatPredicate.ALWAYS_FALSE;
      } else {
        // Compare with floatToIntBits instead of == to canonicalize NaNs.
        final int floatBits = Float.floatToIntBits(valueAsFloat);
        floatPredicate = input -> Float.floatToIntBits(input) == floatBits;
      }
    }
  }

  private void initDoublePredicate()
  {
    if (doublePredicate != null) {
      return;
    }
    synchronized (initLock) {
      if (doublePredicate != null) {
        return;
      }
      if (value == null) {
        doublePredicate = DruidDoublePredicate.MATCH_NULL_ONLY;
        return;
      }
      final Double aDouble = DimensionHandlerUtils.convertObjectToDouble(value);

      if (aDouble == null) {
        doublePredicate = DruidDoublePredicate.ALWAYS_FALSE;
      } else {
        // Compare with doubleToLongBits instead of == to canonicalize NaNs.
        final long bits = Double.doubleToLongBits(aDouble);
        doublePredicate = input -> Double.doubleToLongBits(input) == bits;
      }
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SelectorPredicateFactory that = (SelectorPredicateFactory) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(value);
  }
}
