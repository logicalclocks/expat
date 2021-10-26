/**
 * This file is part of Expat
 * Copyright (C) 2021, Logical Clocks AB. All rights reserved
 *
 * Expat is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * Expat is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with
 * this program. If not, see <https://www.gnu.org/licenses/>.
 *
 */
package io.hops.hopsworks.expat.migrations.reindex;

import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.epipe.EpipeRunner;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.http.HttpHost;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;


public class EpipeReindex implements MigrateStep {
  private static final Logger LOGGER = LogManager.getLogger(EpipeReindex.class);
  private String epipeLocation;
  private List<String> toReindex;
  private HttpHost elastic;
  private String elasticUser;
  private String elasticPass;
  private CloseableHttpClient httpClient;
  
  private void setup()
    throws ConfigurationException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException {
    
    Configuration conf = ConfigurationBuilder.getConfiguration();
  
    String elasticURI = conf.getString(ExpatConf.ELASTIC_URI);
    if (elasticURI == null) {
      throw new ConfigurationException(ExpatConf.ELASTIC_URI + " cannot be null");
    }
    elastic = HttpHost.create(elasticURI);
    
    elasticUser = conf.getString(ExpatConf.ELASTIC_USER_KEY);
    if (elasticUser == null) {
      throw new ConfigurationException(ExpatConf.ELASTIC_USER_KEY + " cannot be null");
    }
    elasticPass = conf.getString(ExpatConf.ELASTIC_PASS_KEY);
    if (elasticPass == null) {
      throw new ConfigurationException(ExpatConf.ELASTIC_PASS_KEY + " cannot be null");
    }
    
    epipeLocation = conf.getString(ExpatConf.EPIPE_PATH);
    if (epipeLocation == null) {
      throw new ConfigurationException(ExpatConf.EPIPE_PATH + " cannot be null");
    }
    toReindex = (List)conf.getList(ExpatConf.EPIPE_REINDEX);
    if (toReindex == null) {
      throw new ConfigurationException(ExpatConf.EPIPE_REINDEX + " cannot be null");
    }
  
    httpClient = HttpClients
      .custom()
      .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(
        CookieSpecs.IGNORE_COOKIES).build())
      .setSSLContext(new SSLContextBuilder().loadTrustMaterial((x509Certificates, s) -> true).build())
      .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
      .build();
  }
  @Override
  public void migrate() throws MigrationException {
    try {
      LOGGER.info("setup");
      setup();
      LOGGER.info("epipe");
      EpipeRunner runner = new EpipeRunner();
      runner.run(httpClient, elastic, elasticUser, elasticPass, epipeLocation, toReindex);
    } catch (IOException | InterruptedException | ConfigurationException | KeyStoreException
      | NoSuchAlgorithmException | KeyManagementException e) {
      throw new MigrationException("epipe error", e);
    }
    LOGGER.info("epipe done");
  }
  
  @Override
  public void rollback() throws RollbackException {
  }
}
