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
package org.apache.pinot.opal.keyCoordinator.api;

import com.google.common.base.Preconditions;
import io.swagger.jaxrs.config.BeanConfig;
import org.apache.pinot.opal.keyCoordinator.helix.KeyCoordinatorClusterHelixManager;
import org.apache.pinot.opal.keyCoordinator.starter.KeyCoordinatorStarter;
import org.glassfish.grizzly.http.server.CLStaticHttpHandler;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;

import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;

public class KeyCoordinatorApiApplication extends ResourceConfig {
  private static final String RESOURCE_PACKAGE = "org.apache.pinot.opal.keyCoordinator.api.resources";

  private URI _baseUri;
  private HttpServer _httpServer;

  public KeyCoordinatorApiApplication(KeyCoordinatorStarter keyCoordinatorStarter) {
    packages(RESOURCE_PACKAGE);
    register(JacksonFeature.class);
    register(new AbstractBinder() {
      @Override
      protected void configure() {
        bind(keyCoordinatorStarter).to(KeyCoordinatorStarter.class);
        bind(keyCoordinatorStarter.getKeyCoordinatorClusterHelixManager()).to(KeyCoordinatorClusterHelixManager.class);
      }
    });
    registerClasses(io.swagger.jaxrs.listing.ApiListingResource.class);
    registerClasses(io.swagger.jaxrs.listing.SwaggerSerializers.class);
  }

  public void start(int httpPort) {
    Preconditions.checkArgument(httpPort > 0);
    _baseUri = URI.create("http://0.0.0.0:" + httpPort + "/");
    _httpServer = GrizzlyHttpServerFactory.createHttpServer(_baseUri, this);
    setupSwagger();
  }

  private void setupSwagger() {
    BeanConfig beanConfig = new BeanConfig();
    beanConfig.setTitle("Pinot key coordinator API");
    beanConfig.setDescription("APIs for accessing Pinot key coordinator information");
    beanConfig.setContact("https://github.com/linkedin/pinot");
    beanConfig.setVersion("1.0");
    beanConfig.setSchemes(new String[]{"http"});
    beanConfig.setBasePath(_baseUri.getPath());
    beanConfig.setResourcePackage(RESOURCE_PACKAGE);
    beanConfig.setScan(true);

    HttpHandler httpHandler = new CLStaticHttpHandler(KeyCoordinatorApiApplication.class.getClassLoader(), "/api/");
    // map both /api and /help to swagger docs. /api because it looks nice. /help for backward compatibility
    _httpServer.getServerConfiguration().addHttpHandler(httpHandler, "/api", "/help");

    URL swaggerDistLocation =
        KeyCoordinatorApiApplication.class.getClassLoader().getResource("META-INF/resources/webjars/swagger-ui/2.2.2/");
    CLStaticHttpHandler swaggerDist = new CLStaticHttpHandler(new URLClassLoader(new URL[]{swaggerDistLocation}));
    _httpServer.getServerConfiguration().addHttpHandler(swaggerDist, "/swaggerui-dist/");
  }

  public void stop() {
    if (_httpServer != null) {
      _httpServer.shutdownNow();
    }
  }
}
