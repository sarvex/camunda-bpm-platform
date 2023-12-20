/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership. Camunda licenses this file to you under the Apache License,
 * Version 2.0; you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.camunda.bpm.engine.impl.history;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.camunda.bpm.engine.ProcessEngineException;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.camunda.bpm.engine.impl.interceptor.CommandContext;
import org.camunda.bpm.engine.impl.persistence.entity.PropertyChange;

/**
 * Interface for Commands that synchronously modify multiple entities in one operation.
 * The methods of this interface take care of producing operation log entries based on the
 * {@link ProcessEngineConfigurationImpl#getLogEntriesPerSyncOperationLimit() logEntriesPerSyncOperationLimit} property.
 */
public interface SynchronousOperationLogProducer<T> {

  public static Long SUMMARY_LOG = 1L;
  public static Long UNLIMITED_LOG = -1L;

  /**
   * Returns a map containing a list of changed properties for every result of the operation.
   * Used to produce an operation log entry per entry contained in the returned map.
   */
  Map<T, List<PropertyChange>> getPropChangesForOperation(List<T> results);

  /**
   * Returns a list of changed properties summarizing the whole operation involving multiple entities.
   */
  List<PropertyChange> getSummarizingPropChangesForOperation(List<T> results);

  /**
   * Calls the code that produces the operation log. Usually <code>commandContext.getOperationLogManager().log...</code>
   */
  void createOperationLogEntry(CommandContext commandContext, T result, List<PropertyChange> propChanges);

  /**
   * The implementing command can call this method to produce the operation log entries for the current operation.
   */
  default void produceOperationLog(CommandContext commandContext, List<T> results) {
    if(results == null || results.isEmpty()) {
      return;
    }
    Map<T, List<PropertyChange>> propChangesForOperation = getPropChangesForOperation(results);

    Long logEntriesPerSyncOperationLimit = commandContext.getProcessEngineConfiguration()
        .getLogEntriesPerSyncOperationLimit();
    if(logEntriesPerSyncOperationLimit == SUMMARY_LOG && results.size() > 1) {
      // use first result as representative for summarized operation log entry
      createOperationLogEntry(commandContext, results.get(0), getSummarizingPropChangesForOperation(results));
    } else {
      if (logEntriesPerSyncOperationLimit != UNLIMITED_LOG && logEntriesPerSyncOperationLimit < propChangesForOperation.size()) {
        throw new ProcessEngineException(
            "Maximum number of operation log entries for operation type synchronous APIs reached. Configured limit is "
                + logEntriesPerSyncOperationLimit + " but " + propChangesForOperation.size() + " entities were affected by API call.");
      } else {
        // produce one operation log per affected entity
        for (Entry<T, List<PropertyChange>> propChanges : propChangesForOperation.entrySet()) {
          createOperationLogEntry(commandContext, propChanges.getKey(), propChanges.getValue());
        }
      }
    }
  }
}
