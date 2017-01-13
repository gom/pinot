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
package com.linkedin.pinot.core.operator.transform;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.operator.docvalsets.ProjectionBlockValSet;
import com.linkedin.pinot.core.operator.transform.function.TransformFunction;
import com.linkedin.pinot.core.operator.transform.function.TransformFunctionFactory;
import com.linkedin.pinot.core.operator.transform.result.TransformResult;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * Class for evaluating transform expression.
 */
public class DefaultExpressionEvaluator implements TransformExpressionEvaluator {

  private final List<TransformExpressionTree> _expressionTrees;

  /**
   * Constructor for the class.
   * @param expressionTrees List of expression trees to evaluate
   */
  public DefaultExpressionEvaluator(@Nonnull List<TransformExpressionTree> expressionTrees) {
    _expressionTrees = expressionTrees;
  }

  /**
   * {@inheritDoc}
   *
   * @param projectionBlock Projection block to evaluate the expression for.
   */
  @Override
  public Map<String, TransformResult> evaluate(ProjectionBlock projectionBlock) {
    Map<String, TransformResult> resultsMap = new HashMap<>(_expressionTrees.size());

    for (TransformExpressionTree expressionTree : _expressionTrees) {
      TransformResult result = evaluateExpression(projectionBlock, expressionTree);
      resultsMap.put(expressionTree.getColumn(), result);
    }
    return resultsMap;
  }

  /**
   * Helper (recursive) method that walks the expression tree bottom up evaluating
   * transforms at each level.
   *
   * @param projectionBlock Projection block for which to evaluate the expression for
   * @param expressionTree Expression tree to evaluate
   * @return Result of the expression transform
   */
  private TransformResult evaluateExpression(ProjectionBlock projectionBlock, TransformExpressionTree expressionTree) {
    TransformFunction function = getTransformFunction(expressionTree.getTransformName());

    if (function != null) {
      List<TransformExpressionTree> children = expressionTree.getChildren();
      int numChildren = children.size();
      Object[] transformArgs = new Object[numChildren];

      for (int i = 0; i < numChildren; i++) {
        transformArgs[i] = evaluateExpression(projectionBlock, children.get(i)).getResult();
      }
      return function.transform(projectionBlock.getNumDocs(), transformArgs);
    } else {
      String column = expressionTree.getColumn();

      if (column != null) {
        BlockValSet blockValSet = projectionBlock.getBlockValueSet(column);
        return buildTransformResult(blockValSet);
      } else {
        throw new RuntimeException("Literals not supported in transforms yet");
      }
    }
  }

  /**
   * Helper method to build TransformResult object for the given set of values.
   *
   * @param blockValSet Value set for which to build the result
   * @return TransformResult containing values from the given value-set.
   */
  private TransformResult buildTransformResult(BlockValSet blockValSet) {
    FieldSpec.DataType valueType = blockValSet.getValueType();

    switch (valueType) {
      case INT:
        return new TransformResult(blockValSet.getIntValuesSV(), valueType);

      case LONG:
        return new TransformResult(blockValSet.getLongValuesSV(), valueType);

      case FLOAT:
        return new TransformResult(blockValSet.getFloatValuesMV(), valueType);

      case DOUBLE:
        return new TransformResult(blockValSet.getDoubleValuesSV(), valueType);

      case STRING:
        return new TransformResult(blockValSet.getStringValuesSV(), valueType);

      default:
        throw new IllegalArgumentException("Illegal data type for transform evaluator: " + valueType);
    }
  }

  /**
   * Helper method to get the transform function from the factory
   *
   * @param transformName Name of transform function
   * @return Instance of transform function
   */
  private TransformFunction getTransformFunction(String transformName) {
    return (transformName != null) ? TransformFunctionFactory.get(transformName) : null;
  }
}
