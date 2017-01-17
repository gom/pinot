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
package com.linkedin.pinot.controller.helix.core.retention;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.SegmentsValidationAndRetentionConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.retention.strategy.RetentionStrategy;
import com.linkedin.pinot.controller.helix.core.retention.strategy.TimeRetentionStrategy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * RetentionManager is scheduled to run only on Leader controller.
 * It will first scan the table configs to get segment retention strategy then
 * do data retention..
 *
 *
 */
public class RetentionManager {
  public static final Logger LOGGER = LoggerFactory.getLogger(RetentionManager.class);

  private final PinotHelixResourceManager _pinotHelixResourceManager;

  private final Map<String, RetentionStrategy> _tableDeletionStrategy = new HashMap<>();
  private final Map<String, List<SegmentZKMetadata>> _segmentMetadataMap = new HashMap<>();
  private final Object _lock = new Object();

  private final ScheduledExecutorService _executorService;
  private final int _runFrequencyInSeconds;

  public RetentionManager(PinotHelixResourceManager pinotHelixResourceManager, int runFrequencyInSeconds) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _runFrequencyInSeconds = runFrequencyInSeconds;
    _executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(@Nonnull Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setName("PinotRetentionManagerExecutorService");
        return thread;
      }
    });
  }

  public void start() {
    scheduleRetentionThreadWithFrequency(_runFrequencyInSeconds);
    LOGGER.info("RetentionManager is started!");
  }

  private void scheduleRetentionThreadWithFrequency(int runFrequencyInSeconds) {
    _executorService.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        synchronized (getLock()) {
          execute();
        }
      }
    }, Math.min(50, runFrequencyInSeconds), runFrequencyInSeconds, TimeUnit.SECONDS);
  }

  private Object getLock() {
    return _lock;
  }

  private void execute() {
    try {
      if (_pinotHelixResourceManager.isLeader()) {
        LOGGER.info("Trying to run retentionManager!");
        updateDeletionStrategiesForEntireCluster();
        LOGGER.info("Finished update deletion strategies for entire cluster!");
        updateSegmentMetadataForEntireCluster();
        LOGGER.info("Finished update segment metadata for entire cluster!");
        scanSegmentMetadataAndPurge();
        LOGGER.info("Finished segment purge for entire cluster!");
      } else {
        LOGGER.info("Not leader of the controller, sleep!");
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while running retention", e);
    }
  }

  private void scanSegmentMetadataAndPurge() {
    for (String tableName : _segmentMetadataMap.keySet()) {
      List<SegmentZKMetadata> segmentZKMetadataList = _segmentMetadataMap.get(tableName);
      for (SegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
        RetentionStrategy deletionStrategy;
        deletionStrategy = _tableDeletionStrategy.get(tableName);
        if (deletionStrategy == null) {
          LOGGER.info("No Retention strategy found for segment: {}", segmentZKMetadata.getSegmentName());
          continue;
        }
        if (segmentZKMetadata instanceof RealtimeSegmentZKMetadata) {
          if (((RealtimeSegmentZKMetadata) segmentZKMetadata).getStatus() == Status.IN_PROGRESS) {
            continue;
          }
        }
        if (deletionStrategy.isPurgeable(segmentZKMetadata)) {
          LOGGER.info("Trying to delete segment: {}", segmentZKMetadata.getSegmentName());
          _pinotHelixResourceManager.deleteSegment(tableName, segmentZKMetadata.getSegmentName());
        }
      }
    }
  }

  private void updateDeletionStrategiesForEntireCluster() {
    List<String> tableNames = _pinotHelixResourceManager.getAllPinotTableNames();
    for (String tableName : tableNames) {
      updateDeletionStrategyForTable(tableName);
    }
  }

  @SuppressWarnings("ConstantConditions")
  private void updateDeletionStrategyForTable(String tableName) {
    switch (TableNameBuilder.getTableTypeFromTableName(tableName)) {
      case OFFLINE:
        updateDeletionStrategyForOfflineTable(tableName);
        break;
      case REALTIME:
        updateDeletionStrategyForRealtimeTable(tableName);
        break;
      default:
        throw new IllegalArgumentException("No table type matches table name: " + tableName);
    }
  }

  /**
   * For offline table, remove the deletion strategy when one of the followings happened:
   * <ul>
   *   <li>Push type changed to some value other than 'APPEND'.</li>
   *   <li>Retention time got removed or changed to some invalid value.</li>
   * </ul>
   */
  private void updateDeletionStrategyForOfflineTable(String offlineTableName) {
    // Fetch table config.
    AbstractTableConfig offlineTableConfig;
    try {
      offlineTableConfig =
          ZKMetadataProvider.getOfflineTableConfig(_pinotHelixResourceManager.getPropertyStore(), offlineTableName);
      if (offlineTableConfig == null) {
        LOGGER.error("Table config is null, skip updating deletion strategy for table: {}.", offlineTableName);
        return;
      }
    } catch (Exception e) {
      LOGGER.error(
          "Caught exception while getting table config from property store, skip updating deletion strategy for table: {}.",
          offlineTableName, e);
      return;
    }

    // Fetch validation config.
    SegmentsValidationAndRetentionConfig validationConfig = offlineTableConfig.getValidationConfig();
    if (validationConfig == null) {
      LOGGER.error("Validation config is null, skip updating deletion strategy for table: {}.", offlineTableName);
      return;
    }

    // Fetch push type.
    if ((validationConfig.getSegmentPushType() == null) || (!validationConfig.getSegmentPushType()
        .equalsIgnoreCase("APPEND"))) {
      LOGGER.info("Segment push type is not 'APPEND', remove deletion strategy for table: {}.", offlineTableName);
      _tableDeletionStrategy.remove(offlineTableName);
      return;
    }

    // Fetch retention time unit and value.
    String retentionTimeUnit = validationConfig.getRetentionTimeUnit();
    String retentionTimeValue = validationConfig.getRetentionTimeValue();
    if (((retentionTimeUnit == null) || retentionTimeUnit.isEmpty()) || ((retentionTimeValue == null)
        || retentionTimeValue.isEmpty())) {
      LOGGER.info("Retention time unit/value is not set, remove deletion strategy for table: {}.", offlineTableName);
      _tableDeletionStrategy.remove(offlineTableName);
      return;
    }

    // Update time retention strategy.
    try {
      TimeRetentionStrategy timeRetentionStrategy = new TimeRetentionStrategy(retentionTimeUnit, retentionTimeValue);
      _tableDeletionStrategy.put(offlineTableName, timeRetentionStrategy);
      LOGGER.info("Updated retention strategy for table: {} using retention time: {} {}.", offlineTableName,
          retentionTimeValue, retentionTimeUnit);
    } catch (Exception e) {
      LOGGER.error(
          "Caught exception while building retention strategy with retention time: {} {], remove deletion strategy for table: {}.",
          retentionTimeValue, retentionTimeUnit, offlineTableName);
      _tableDeletionStrategy.remove(offlineTableName);
    }
  }

  /**
   * For real-time table, only update but never remove deletion strategy.
   */
  @SuppressWarnings("ConstantConditions")
  private void updateDeletionStrategyForRealtimeTable(String realtimeTableName) {
    try {
      AbstractTableConfig realtimeTableConfig =
          ZKMetadataProvider.getRealtimeTableConfig(_pinotHelixResourceManager.getPropertyStore(), realtimeTableName);
      SegmentsValidationAndRetentionConfig validationConfig = realtimeTableConfig.getValidationConfig();
      TimeRetentionStrategy timeRetentionStrategy =
          new TimeRetentionStrategy(validationConfig.getRetentionTimeUnit(), validationConfig.getRetentionTimeValue());
      _tableDeletionStrategy.put(realtimeTableName, timeRetentionStrategy);
    } catch (Exception e) {
      LOGGER.error("Caught exception while updating deletion strategy, skip updating deletion strategy for table: {}.",
          realtimeTableName, e);
    }
  }

  private void updateSegmentMetadataForEntireCluster() {
    List<String> tableNames = _pinotHelixResourceManager.getAllPinotTableNames();
    for (String tableName : tableNames) {
      _segmentMetadataMap.put(tableName, retrieveSegmentMetadataForTable(tableName));
    }
  }

  @SuppressWarnings("ConstantConditions")
  private List<SegmentZKMetadata> retrieveSegmentMetadataForTable(String tableName) {
    List<SegmentZKMetadata> segmentMetadataList = new ArrayList<>();
    ZkHelixPropertyStore<ZNRecord> propertyStore = _pinotHelixResourceManager.getPropertyStore();
    switch (TableNameBuilder.getTableTypeFromTableName(tableName)) {
      case OFFLINE:
        List<OfflineSegmentZKMetadata> offlineSegmentZKMetadatas =
            ZKMetadataProvider.getOfflineSegmentZKMetadataListForTable(propertyStore, tableName);
        for (OfflineSegmentZKMetadata offlineSegmentZKMetadata : offlineSegmentZKMetadatas) {
          segmentMetadataList.add(offlineSegmentZKMetadata);
        }
        break;
      case REALTIME:
        List<RealtimeSegmentZKMetadata> realtimeSegmentZKMetadatas =
            ZKMetadataProvider.getRealtimeSegmentZKMetadataListForTable(propertyStore, tableName);
        for (RealtimeSegmentZKMetadata realtimeSegmentZKMetadata : realtimeSegmentZKMetadatas) {
          segmentMetadataList.add(realtimeSegmentZKMetadata);
        }
        break;
      default:
        throw new IllegalArgumentException("No table type matches table name: " + tableName);
    }
    return segmentMetadataList;
  }

  public void stop() {
    _executorService.shutdown();
  }
}
