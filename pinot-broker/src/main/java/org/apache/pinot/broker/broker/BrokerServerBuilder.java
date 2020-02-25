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
package org.apache.pinot.broker.broker;

import com.google.common.base.Preconditions;
import org.apache.pinot.broker.upsert.LowWaterMarkService;
import com.yammer.metrics.core.MetricsRegistry;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.requesthandler.BrokerRequestHandler;
import org.apache.pinot.broker.requesthandler.SingleConnectionBrokerRequestHandler;
import org.apache.pinot.broker.routing.RoutingTable;
import org.apache.pinot.broker.routing.TimeBoundaryService;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.MetricsHelper;
import org.apache.pinot.common.utils.CommonConstants.Broker;
import org.apache.pinot.common.utils.CommonConstants.Helix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;


public class BrokerServerBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerServerBuilder.class);

  public enum State {
    INIT, STARTING, RUNNING, SHUTTING_DOWN, SHUTDOWN
  }

  // Running State Of broker
  private final AtomicReference<State> _state = new AtomicReference<>(State.INIT);

  private final Configuration _config;
  private final long _delayedShutdownTimeMs;
  private final RoutingTable _routingTable;
  private final TimeBoundaryService _timeBoundaryService;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final AccessControlFactory _accessControlFactory;
  private final MetricsRegistry _metricsRegistry;
  private final BrokerMetrics _brokerMetrics;
  private final BrokerRequestHandler _brokerRequestHandler;
  private final BrokerAdminApiApplication _brokerAdminApplication;
  private final LowWaterMarkService _lwmService;

  public BrokerServerBuilder(Configuration config, RoutingTable routingTable, TimeBoundaryService timeBoundaryService,
      QueryQuotaManager queryQuotaManager, ZkHelixPropertyStore<ZNRecord> propertyStore,
                             LowWaterMarkService lowWaterMarkService) {
    _config = config;
    _delayedShutdownTimeMs =
        config.getLong(Broker.CONFIG_OF_DELAY_SHUTDOWN_TIME_MS, Broker.DEFAULT_DELAY_SHUTDOWN_TIME_MS);
    _routingTable = routingTable;
    _timeBoundaryService = timeBoundaryService;
    _propertyStore = propertyStore;
    _accessControlFactory = AccessControlFactory.loadFactory(_config.subset(Broker.ACCESS_CONTROL_CONFIG_PREFIX));
    _metricsRegistry = new MetricsRegistry();
    MetricsHelper.initializeMetrics(config.subset(Broker.METRICS_CONFIG_PREFIX));
    MetricsHelper.registerMetricsRegistry(_metricsRegistry);
    _brokerMetrics =
        new BrokerMetrics(config.getString(Broker.CONFIG_OF_METRICS_NAME_PREFIX, Broker.DEFAULT_METRICS_NAME_PREFIX),
            _metricsRegistry,
            !_config.getBoolean(Broker.CONFIG_OF_ENABLE_TABLE_LEVEL_METRICS, !Broker.DEFAULT_METRICS_GLOBAL_ENABLED));
    _brokerMetrics.initializeGlobalMeters();
    _brokerRequestHandler =
        new SingleConnectionBrokerRequestHandler(_config, _routingTable, _timeBoundaryService, _accessControlFactory,
            queryQuotaManager, _brokerMetrics, _propertyStore, lowWaterMarkService);
    _brokerAdminApplication = new BrokerAdminApiApplication(this);

    _lwmService = lowWaterMarkService;
  }

  public void start() {
    LOGGER.info("Starting Pinot Broker");

    Preconditions.checkState(_state.get() == State.INIT);
    _state.set(State.STARTING);

    _brokerRequestHandler.start();
    int brokerQueryPort = _config.getInt(Helix.KEY_OF_BROKER_QUERY_PORT, Helix.DEFAULT_BROKER_QUERY_PORT);
    _brokerAdminApplication.start(brokerQueryPort);

    _state.set(State.RUNNING);
    LOGGER.info("Pinot Broker is started and listening on port {} for API requests", brokerQueryPort);
  }

  public void stop() {
    LOGGER.info("Shutting down Pinot Broker");

    try {
      Thread.sleep(_delayedShutdownTimeMs);
    } catch (Exception e) {
      LOGGER.error("Caught exception while waiting for shutdown delay period of {}ms", _delayedShutdownTimeMs, e);
    }

    Preconditions.checkState(_state.get() == State.RUNNING);
    _state.set(State.SHUTTING_DOWN);

    _brokerRequestHandler.shutDown();
    _brokerAdminApplication.stop();

    _state.set(State.SHUTDOWN);
    LOGGER.info("Finish shutting down Pinot Broker");
  }

  public State getCurrentState() {
    return _state.get();
  }

  public RoutingTable getRoutingTable() {
    return _routingTable;
  }

  public TimeBoundaryService getTimeBoundaryService() {
    return _timeBoundaryService;
  }

  public AccessControlFactory getAccessControlFactory() {
    return _accessControlFactory;
  }

  public MetricsRegistry getMetricsRegistry() {
    return _metricsRegistry;
  }

  public BrokerMetrics getBrokerMetrics() {
    return _brokerMetrics;
  }

  public BrokerRequestHandler getBrokerRequestHandler() {
    return _brokerRequestHandler;
  }
}
