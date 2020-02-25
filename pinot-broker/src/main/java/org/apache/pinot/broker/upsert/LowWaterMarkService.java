/**
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
package org.apache.pinot.broker.upsert;

import org.apache.pinot.common.metrics.BrokerMetrics;

import java.util.Map;

/**
 * LowWaterMarkService keeps records of the low water mark (i.e., the stream ingestion progress) for each partition of
 * an input table.
 */
public interface LowWaterMarkService {
    // Return the low water mark mapping from partition id to the corresponding low water mark of a given table.
    Map<Integer, Long> getLowWaterMarks(String tableName);

    // Shutdown the service.
    void shutDown();

    // start
    void start(BrokerMetrics brokerMetrics);
}
