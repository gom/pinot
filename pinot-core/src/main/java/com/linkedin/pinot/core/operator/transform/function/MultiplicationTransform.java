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
package com.linkedin.pinot.core.operator.transform.function;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.operator.transform.result.TransformResult;
import java.util.Arrays;
import javax.annotation.Nonnull;


/**
 * This class implements the Multiplication transform function.
 */
public class MultiplicationTransform implements TransformFunction {
  private static final String TRANSFORM_NAME = "mult";
  private double[] _product;

  /**
   * {@inheritDoc}
   *
   * @param length Length of doc ids to process
   * @param inputs Array of input values
   * @return TransformResult containing result values
   */
  @Override
  public TransformResult transform(int length, @Nonnull Object... inputs) {
    if (_product == null) {
      _product = new double[length];
    }

    Arrays.fill(_product, 1);
    for (Object input : inputs) {
      double[] values = (double[]) input;
      for (int j = 0; j < length; j++) {
        _product[j] *= values[j];
      }
    }
    return new TransformResult(_product, FieldSpec.DataType.DOUBLE);
  }

  /**
   * {@inheritDoc}
   *
   * @return {@link #TRANSFORM_NAME}
   */
  @Override
  public String getName() {
    return TRANSFORM_NAME;
  }
}
