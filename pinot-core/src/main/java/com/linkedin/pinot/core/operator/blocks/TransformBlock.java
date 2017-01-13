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
package com.linkedin.pinot.core.operator.blocks;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.operator.docvalsets.TransformBlockValSet;
import com.linkedin.pinot.core.operator.transform.result.TransformResult;
import java.util.Map;


/**
 * Transform Block holds blocks of transformed columns. In absence of transforms,
 * it servers as a pass-through to projection block.
 */
public class TransformBlock implements Block {
  private final Map<String, TransformResult> _transformResult;
  private ProjectionBlock _projectionBlock;

  @Override
  public boolean applyPredicate(Predicate predicate) {
    throw new UnsupportedOperationException();
  }

  public TransformBlock(ProjectionBlock projectionBlock, Map<String, TransformResult> transformResults) {
    _projectionBlock = projectionBlock;
    _transformResult = transformResults;
  }

  @Override
  public BlockId getId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockValSet getBlockValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockMetadata getMetadata() {
    return _projectionBlock.getMetadata();
  }

  public BlockValSet getBlockValueSet(String column) {
    TransformResult transformResult = _transformResult.get(column);
    if (transformResult != null) {
      return new TransformBlockValSet(transformResult);
    } else {
      return _projectionBlock.getBlockValueSet(column);
    }
  }

  public BlockMetadata getBlockMetadata(String column) {
    return _projectionBlock.getMetadata(column);
  }

  public DocIdSetBlock getDocIdSetBlock() {
    return _projectionBlock.getDocIdSetBlock();
  }

  public int getNumDocs() {
    return _projectionBlock.getNumDocs();
  }

  /**
   * This method returns a map containing transform expressions as keys and the result of
   * applying those expressions as values.
   *
   * @return Result of Transform
   */
  public Map<String, TransformResult> getTransformResults() {
    return _transformResult;
  }
}
