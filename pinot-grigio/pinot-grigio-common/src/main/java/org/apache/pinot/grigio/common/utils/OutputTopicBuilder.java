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
package org.apache.pinot.grigio.common.utils;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

public class OutputTopicBuilder {
  private final boolean _useSingleTopic;
  private String _topicPrefix;
  private String _outputTopic;

  public OutputTopicBuilder(boolean useSingleTopic, String prefix, String topic) {
    _useSingleTopic = useSingleTopic;
    if (_useSingleTopic) {
      _outputTopic = topic;
      Preconditions.checkState(StringUtils.isNotEmpty(_outputTopic),
          "key coordinator output topic is empty while configured to use single topic");
    } else {
      _topicPrefix = prefix;
      Preconditions.checkState(StringUtils.isNotEmpty(_outputTopic),
          "key coordinator output topic prefix is empty while configured to use multiple topic");
    }
  }

  public String getOutputTopic(String tableName) {
    if (_useSingleTopic) {
      return _outputTopic;
    } else {
      return _topicPrefix + tableName;
    }
  }

}
