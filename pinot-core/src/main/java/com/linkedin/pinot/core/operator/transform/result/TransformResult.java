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
package com.linkedin.pinot.core.operator.transform.result;

import com.linkedin.pinot.common.data.FieldSpec;


/**
 * Interface for representing the results of a transform function.
 */
public class TransformResult {
  private final FieldSpec.DataType _resultType;
  private final Object _result;

  public TransformResult(Object result, FieldSpec.DataType resultType) {
    _result = result;
    _resultType = resultType;
  }

  /**
   * Returns the underlying array storage for the results.
   * Client responsible for passing the correct instance of T.
   *
   * @return Result array containing results of transform.
   */
  public <T> T getResult() {
    return (T) _result;
  }

  /**
   * Returns the data type for the result.
   *
   * @return Data type for the result.
   */
  public FieldSpec.DataType getResultType() {
    return _resultType;
  }
}
