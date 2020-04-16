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
package org.apache.pinot.core.segment.updater;

public class SegmentUpdaterConfig {
  public static final String SEGMENT_UDPATE_SLEEP_MS = "sleep.ms";
  public static final String CONFIG_USE_SINGLE_INPUT_TOPIC = "input.topic.singleTopic";
  public static final boolean DEFAULT_USE_SINGLE_INPUT_TOPIC = true;
  public static final String CONFIG_SINGLE_INPUT_TOPIC_NAME = "input.topic.name";
  public static final String INPUT_TOPIC_PREFIX  = "input.topic.prefix";
  public static final int SEGMENT_UDPATE_SLEEP_MS_DEFAULT = 100;
}
