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
package com.linkedin.pinot.core.operator.docvalsets;

import com.clearspring.analytics.util.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.BaseBlockValSet;
import com.linkedin.pinot.core.operator.transform.result.TransformResult;


/**
 * This class represents the BlockValSet for a TransformBlock for a specific column.
 * It provides api's to access data specific to the column.
 */
public class TransformBlockValSet extends BaseBlockValSet {

  private final Object _values;
  private final FieldSpec.DataType _dataType;

  public TransformBlockValSet(TransformResult transformResult) {
    _values = transformResult.getResult();
    _dataType = transformResult.getResultType();
  }

  @Override
  public int[] getIntValuesSV() {
    Preconditions.checkState(_dataType == FieldSpec.DataType.INT);
    return (int[]) _values;
  }

  @Override
  public long[] getLongValuesSV() {
    Preconditions.checkState(_dataType == FieldSpec.DataType.LONG);
    return (long[]) _values;
  }

  @Override
  public float[] getFloatValuesSV() {
    Preconditions.checkState(_dataType == FieldSpec.DataType.FLOAT);
    return (float[]) _values;
  }

  @Override
  public double[] getDoubleValuesSV() {
    Preconditions.checkState(_dataType == FieldSpec.DataType.DOUBLE);
    return (double[]) _values;
  }

  @Override
  public String[] getStringValuesSV() {
    Preconditions.checkArgument(_dataType == FieldSpec.DataType.STRING);
    return (String[]) _values;
  }

  @Override
  public FieldSpec.DataType getValueType() {
    return _dataType;
  }
}
