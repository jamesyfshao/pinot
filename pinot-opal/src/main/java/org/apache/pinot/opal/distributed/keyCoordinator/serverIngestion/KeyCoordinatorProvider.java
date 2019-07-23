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
package org.apache.pinot.opal.distributed.keyCoordinator.serverIngestion;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.opal.common.Config.CommonConfig;
import org.apache.pinot.opal.common.RpcQueue.QueueProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KeyCoordinatorProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyCoordinatorProvider.class);
  private static KeyCoordinatorProvider _instance = null;

  private Configuration _conf;
  private KeyCoordinatorQueueProducer _producer;

  public KeyCoordinatorProvider(Configuration conf, String hostname) {
    Preconditions.checkState(StringUtils.isNotEmpty(hostname), "host name should not be empty");
    _conf = conf;
    // TODO: make this injectable
    _producer = new KeyCoordinatorQueueProducer(conf.subset(CommonConfig.KAFKA_CONFIG.PRODUCER_CONFIG_KEY), hostname);
    synchronized (KeyCoordinatorProvider.class) {
      if (_instance == null) {
        _instance = this;
      } else {
        throw new RuntimeException("cannot re-initialize key coordinator provide when there is already one instance");
      }
    }
  }

  public static KeyCoordinatorProvider getInstance() {
    if (_instance != null) {
      return _instance;
    } else {
      throw new RuntimeException("cannot get instance of key coordinator provider without initializing one before");
    }
  }

  public QueueProducer getProducer() {
    return _producer;
  }

  public void close() {
    //TODO close producer and what not
  }
}
