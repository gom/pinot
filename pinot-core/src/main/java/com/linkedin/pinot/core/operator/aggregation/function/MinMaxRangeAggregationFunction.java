/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.aggregation.function;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.operator.aggregation.ObjectAggregationResultHolder;
import com.linkedin.pinot.core.operator.aggregation.groupby.GroupByResultHolder;
import com.linkedin.pinot.core.operator.aggregation.groupby.ObjectGroupByResultHolder;
import com.linkedin.pinot.core.query.aggregation.function.MinMaxRangeAggregationFunction.MinMaxRangePair;
import javax.annotation.Nonnull;


public class MinMaxRangeAggregationFunction implements AggregationFunction<MinMaxRangePair, Double> {
  private static final String NAME = AggregationFunctionFactory.AggregationFunctionType.MINMAXRANGE.getName();
  private static final double DEFAULT_FINAL_RESULT = Double.NEGATIVE_INFINITY;

  @Nonnull
  @Override
  public String getName() {
    return NAME;
  }

  @Nonnull
  @Override
  public String getColumnName(@Nonnull String[] columns) {
    return NAME + "_" + columns[0];
  }

  @Override
  public void accept(@Nonnull AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  @Nonnull
  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Nonnull
  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity, int trimSize) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity, trimSize);
  }

  @Override
  public void aggregate(int length, @Nonnull AggregationResultHolder aggregationResultHolder,
      @Nonnull BlockValSet... blockValSets) {
    double[] valueArray = blockValSets[0].getDoubleValuesSV();
    double min = Double.POSITIVE_INFINITY;
    double max = Double.NEGATIVE_INFINITY;
    for (int i = 0; i < length; i++) {
      double value = valueArray[i];
      if (value < min) {
        min = value;
      }
      if (value > max) {
        max = value;
      }
    }
    setAggregationResult(aggregationResultHolder, min, max);
  }

  protected void setAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder, double min,
      double max) {
    // TODO: make a class for the pair which implements comparable and stores mutable primitive values.
    MinMaxRangePair minMaxRangePair = aggregationResultHolder.getResult();
    if (minMaxRangePair == null) {
      aggregationResultHolder.setValue(new MinMaxRangePair(min, max));
    } else {
      if (min < minMaxRangePair.getFirst()) {
        minMaxRangePair.setFirst(min);
      }
      if (max > minMaxRangePair.getSecond()) {
        minMaxRangePair.setSecond(max);
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, @Nonnull int[] groupKeyArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    double[] valueArray = blockValSets[0].getDoubleValuesSV();
    for (int i = 0; i < length; i++) {
      double value = valueArray[i];
      setGroupByResult(groupKeyArray[i], groupByResultHolder, value, value);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, @Nonnull int[][] groupKeysArray,
      @Nonnull GroupByResultHolder groupByResultHolder, @Nonnull BlockValSet... blockValSets) {
    double[] valueArray = blockValSets[0].getDoubleValuesSV();
    for (int i = 0; i < length; i++) {
      double value = valueArray[i];
      for (int groupKey : groupKeysArray[i]) {
        setGroupByResult(groupKey, groupByResultHolder, value, value);
      }
    }
  }

  protected void setGroupByResult(int groupKey, @Nonnull GroupByResultHolder groupByResultHolder, double min,
      double max) {
    MinMaxRangePair minMaxRangePair = groupByResultHolder.getResult(groupKey);
    if (minMaxRangePair == null) {
      groupByResultHolder.setValueForKey(groupKey, new MinMaxRangePair(min, max));
    } else {
      if (min < minMaxRangePair.getFirst()) {
        minMaxRangePair.setFirst(min);
      }
      if (max > minMaxRangePair.getSecond()) {
        minMaxRangePair.setSecond(max);
      }
    }
  }

  @Nonnull
  @Override
  public MinMaxRangePair extractAggregationResult(@Nonnull AggregationResultHolder aggregationResultHolder) {
    MinMaxRangePair minMaxRangePair = aggregationResultHolder.getResult();
    if (minMaxRangePair == null) {
      return new MinMaxRangePair(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
    } else {
      return minMaxRangePair;
    }
  }

  @Nonnull
  @Override
  public MinMaxRangePair extractGroupByResult(@Nonnull GroupByResultHolder groupByResultHolder, int groupKey) {
    MinMaxRangePair minMaxRangePair = groupByResultHolder.getResult(groupKey);
    if (minMaxRangePair == null) {
      return new MinMaxRangePair(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
    } else {
      return minMaxRangePair;
    }
  }

  @Nonnull
  @Override
  public MinMaxRangePair merge(@Nonnull MinMaxRangePair intermediateResult1,
      @Nonnull MinMaxRangePair intermediateResult2) {
    double min = intermediateResult2.getFirst();
    if (min < intermediateResult1.getFirst()) {
      intermediateResult1.setFirst(min);
    }
    double max = intermediateResult2.getSecond();
    if (max > intermediateResult1.getSecond()) {
      intermediateResult1.setSecond(max);
    }
    return intermediateResult1;
  }

  @Nonnull
  @Override
  public FieldSpec.DataType getIntermediateResultDataType() {
    return FieldSpec.DataType.OBJECT;
  }

  @Nonnull
  @Override
  public Double extractFinalResult(@Nonnull MinMaxRangePair intermediateResult) {
    double min = intermediateResult.getFirst();
    double max = intermediateResult.getSecond();
    if (min > max) {
      return DEFAULT_FINAL_RESULT;
    } else {
      return max - min;
    }
  }
}
