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

package org.apache.druid.segment.filter;

import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.index.AllFalseBitmapColumnIndex;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class FalseFilter implements Filter
{
  private static final FalseFilter INSTANCE = new FalseFilter();

  public static FalseFilter instance()
  {
    return INSTANCE;
  }

  private FalseFilter()
  {
  }

  @Nullable
  @Override
  public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
  {
    return new AllFalseBitmapColumnIndex(selector.getBitmapFactory());
  }

  @Override
  public double estimateSelectivity(ColumnIndexSelector indexSelector)
  {
    return 0;
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return ConstantMatcherType.ALL_FALSE.asValueMatcher();
  }

  @Override
  public VectorValueMatcher makeVectorMatcher(VectorColumnSelectorFactory factory)
  {
    return ConstantMatcherType.ALL_FALSE.asVectorMatcher(factory.getReadableVectorInspector());
  }

  @Override
  public boolean supportsSelectivityEstimation(ColumnSelector columnSelector, ColumnIndexSelector indexSelector)
  {
    return true;
  }

  @Override
  public boolean canVectorizeMatcher(ColumnInspector inspector)
  {
    return true;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return Collections.emptySet();
  }

  @Override
  public boolean supportsRequiredColumnRewrite()
  {
    return true;
  }

  @Override
  public Filter rewriteRequiredColumns(Map<String, String> columnRewrites)
  {
    return this;
  }

  @Override
  public String toString()
  {
    return "false";
  }

  @Override
  public final int hashCode()
  {
    return FalseFilter.class.hashCode();
  }

  @Override
  public final boolean equals(Object obj)
  {
    return obj instanceof FalseFilter;
  }
}
