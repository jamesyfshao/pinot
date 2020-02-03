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
package org.apache.pinot.grigio.keyCoordinator.starter;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.grigio.common.config.CommonConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class KeyCoordinatorConf extends PropertiesConfiguration {

  public static final String FETCH_MSG_DELAY_MS = "kc.queue.fetch.delay.ms";
  public static final int FETCH_MSG_DELAY_MS_DEFAULT = 100;

  public static final String FETCH_MSG_MAX_DELAY_MS = "kc.queue.fetch.delay.max.ms";
  public static final int FETCH_MSG_MAX_DELAY_MS_DEFAULT = 5000;

  public static final String FETCH_MSG_MAX_BATCH_SIZE = "kc.queue.fetch.size";
  public static final int FETCH_MSG_MAX_BATCH_SIZE_DEFAULT = 10000;

  public static final String CONSUMER_BLOCKING_QUEUE_SIZE = "consumer.queue.size";
  public static final int CONSUMER_BLOCKING_QUEUE_SIZE_DEFAULT = 10000;

  public static final String VERSION_MESSAGE_INTERVAL_MS = "version.message.interval.ms";
  public static final long VERSION_MESSAGE_INTERVAL_MS_DEFAULT = 1000;

  public static final String KEY_COORDINATOR_KV_STORE = "kvstore";

  // server related config
  public static final String SERVER_CONFIG = "web.server";
  public static final String PORT = "jersey.port";
  public static final int PORT_DEFAULT = 8092;
  public static final String HOST_NAME = "hostname";

  // storage provider config
  public static final String STORAGE_PROVIDER_CONFIG = "updatelog.storage";

  // kafka prefix
  public static final String KAFKA_CLIENT_ID_PREFIX = "pinot_upsert_client_";
  public static final String KAFKA_CONSUMER_GROUP_ID_PREFIX = "pinot_upsert_kc_consumerGroup_";

  private static final String KC_MESSAGE_TOPIC = "kc.message.topic";
  private static final String KC_MESSAGE_PARTITION_COUNT = "kc.message.partition.count";

  public static final String KC_OUTPUT_TOPIC_PREFIX_KEY = "kc.output.topic.prefix";

  // helix related cofig
  private static final String HELIX_CLUSTER_NAME = "helix.cluster.name";
  private static final String ZK_STR = "zk.str";
  private static final String KC_CLUSTER_NAME = "kc.cluster.name";

  private static final String PINOT_HELIX_CLUSTER_NAME = "pinot.helix.cluster.name";
  private static final String PINOT_HELIX_ZK_STR = "pinot.zk.str";

  // metrics related config
  public static final String METRICS_CONFIG = "metrics";

  // local integration testing config
  // set this flag to be true so we can ensure all pinot component run within a single JVM
  public static final String LOCAL_TESTING_MODE = "localtest.enable";

  public KeyCoordinatorConf(File file) throws ConfigurationException {
    super(file);
  }

  public KeyCoordinatorConf() {
    super();
  }

  public int getConsumerBlockingQueueSize() {
    return getInt(CONSUMER_BLOCKING_QUEUE_SIZE, CONSUMER_BLOCKING_QUEUE_SIZE_DEFAULT);
  }

  public Configuration getProducerConf() {
    return this.subset(CommonConfig.RPC_QUEUE_CONFIG.PRODUCER_CONFIG_KEY);
  }

  public Configuration getConsumerConf() {
    return this.subset(CommonConfig.RPC_QUEUE_CONFIG.CONSUMER_CONFIG_KEY);
  }

  public Configuration getVersionMessageProducerConf() {
    return this.subset(CommonConfig.RPC_QUEUE_CONFIG.VERSION_MESSAGE_PRODUCER_CONFIG_KEY);
  }

  public Configuration getMetricsConf() {
    return this.subset(METRICS_CONFIG);
  }

  public Configuration getServerConf() {
    return this.subset(SERVER_CONFIG);
  }

  public Configuration getStorageProviderConf() {
    return this.subset(STORAGE_PROVIDER_CONFIG);
  }

  public int getPort() {
    return this.subset(SERVER_CONFIG).getInt(KeyCoordinatorConf.PORT, KeyCoordinatorConf.PORT_DEFAULT);
  }

  public String getTopicPrefix() {
    return this.getString(KC_OUTPUT_TOPIC_PREFIX_KEY);
  }

  public String getKeyCoordinatorClusterName() {
    return this.getString(KC_CLUSTER_NAME);
  }

  public String getZkStr() {
    return convertConfigToZkString(getProperty(ZK_STR));
  }

  public String getPinotClusterZkStr() {
    return convertConfigToZkString(getProperty(PINOT_HELIX_ZK_STR));
  }

  public String getPinotClusterName() {
    return this.getString(PINOT_HELIX_CLUSTER_NAME);
  }

  // convert the config value to zk string
  private String convertConfigToZkString(Object zkAddressObj) {
    // The set method converted comma separated string into ArrayList, so need to convert back to String here.
    if (zkAddressObj instanceof ArrayList) {
      List<String> zkAddressList = (ArrayList<String>) zkAddressObj;
      String[] zkAddress = zkAddressList.toArray(new String[0]);
      return StringUtil.join(",", zkAddress);
    } else if (zkAddressObj instanceof String) {
      return (String) zkAddressObj;
    } else {
      throw new RuntimeException(
          "Unexpected data type for zkAddress PropertiesConfiguration, expecting String but got " + zkAddressObj
              .getClass().getName());
    }

  }

  public String getKeyCoordinatorMessageTopic() {
    return this.getString(KC_MESSAGE_TOPIC);
  }

  public int getKeyCoordinatorMessagePartitionCount() {
    return this.getInt(KC_MESSAGE_PARTITION_COUNT);
  }

  public String getMetricsPrefix() {
    return getString(CommonConstants.Grigio.CONFIG_OF_METRICS_PREFIX_KEY, CommonConstants.Grigio.DEFAULT_METRICS_PREFIX);
  }

  // return true if we are configuring the current key coordinator to be ran as an component within JVM for test
  // expect some component to have custom start up logic
  public boolean isLocalTestingMode() {
    try {
      return getBoolean(LOCAL_TESTING_MODE, false);
    } catch (Exception ex) {
      return false;
    }
  }
}
