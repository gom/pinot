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
package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.core.operator.transform.TransformExpressionOperator;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>TransformPlanNode</code> class provides the execution plan for transforms on a single segment.
 */
public class TransformPlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransformPlanNode.class);

  private final IndexSegment _indexSegment;
  private final ProjectionPlanNode _projectionPlanNode;
  private final List<TransformExpressionTree> _transformExpressions;

  /**
   * Constructor for the class
   *
   * @param indexSegment Segment to process
   * @param brokerRequest BrokerRequest to process
   */
  public TransformPlanNode(@Nonnull IndexSegment indexSegment, @Nonnull BrokerRequest brokerRequest) {
    _indexSegment = indexSegment;

    Set<String> projectionColumns = new HashSet<>();
    Set<String> transformExpressions = new HashSet<>();
    extractColumnsAndTransforms(brokerRequest, projectionColumns, transformExpressions);
    _transformExpressions = buildTransformExpressionTrees(transformExpressions);

    _projectionPlanNode = new ProjectionPlanNode(_indexSegment,
        projectionColumns.toArray(new String[projectionColumns.size()]),
        new DocIdSetPlanNode(_indexSegment, brokerRequest));
  }

  private List<TransformExpressionTree> buildTransformExpressionTrees(Set<String> transformExpressions) {
    Pql2Compiler compiler = new Pql2Compiler();
    List<TransformExpressionTree> expressionTrees = new ArrayList<>(transformExpressions.size());

    for (String transformExpression : transformExpressions) {
      TransformExpressionTree expressionTree = compiler.compileToExpressionTree(transformExpression);
      expressionTrees.add(expressionTree);
    }
    return expressionTrees;
  }

  /**
   * Helper method to extract projection columns and transform expressions from the given
   * BrokerRequest.
   *
   * @param brokerRequest BrokerRequest to process
   * @param projectionColumns Output projection columns from broker request
   * @param transformExpressions Output transform expression from broker request
   */
  private void extractColumnsAndTransforms(BrokerRequest brokerRequest, Set<String> projectionColumns,
      Set<String> transformExpressions) {

    if (brokerRequest.isSetAggregationsInfo()) {
      for (AggregationInfo aggregationInfo : brokerRequest.getAggregationsInfo()) {
        if (!aggregationInfo.getAggregationType()
            .equalsIgnoreCase(AggregationFunctionFactory.AggregationFunctionType.COUNT.getName())) {
          String columns = aggregationInfo.getAggregationParams().get("column").trim();
          projectionColumns.addAll(Arrays.asList(columns.split(",")));
        }
      }

      // Collect all group by related columns.
      if (brokerRequest.isSetGroupBy()) {
        GroupBy groupBy = brokerRequest.getGroupBy();
        projectionColumns.addAll(groupBy.getColumns());
        List<String> expressions = groupBy.getExpressions();

        // Check null for backward compatibility.
        if (expressions != null) {
          transformExpressions.addAll(expressions);
        }
      }
    } else {
      projectionColumns.addAll(brokerRequest.getSelections().getSelectionColumns());
    }
  }

  @Override
  public Operator run() {
    MProjectionOperator projectionOperator = (MProjectionOperator) _projectionPlanNode.run();
    return new TransformExpressionOperator(projectionOperator, _transformExpressions);
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Segment Level Inner-Segment Plan Node:");
    LOGGER.debug(prefix + "Operator: TransformOperator");
    LOGGER.debug(prefix + "Argument 0: IndexSegment - " + _indexSegment.getSegmentName());
    LOGGER.debug(prefix + "Argument 1: Projection -");
    _projectionPlanNode.showTree(prefix + "    ");
  }
}
