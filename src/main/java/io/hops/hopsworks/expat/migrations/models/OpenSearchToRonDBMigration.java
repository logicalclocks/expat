package io.hops.hopsworks.expat.migrations.models;
/**
 * This file is part of Expat
 * Copyright (C) 2023, Logical Clocks AB. All rights reserved
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

import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.db.dao.hdfs.inode.ExpatHdfsInode;
import io.hops.hopsworks.expat.db.dao.hdfs.inode.ExpatInodeController;
import io.hops.hopsworks.expat.db.dao.models.ExpatModel;
import io.hops.hopsworks.expat.db.dao.models.ExpatModelVersion;
import io.hops.hopsworks.expat.db.dao.models.ExpatModelsController;
import io.hops.hopsworks.expat.db.dao.project.ExpatProject;
import io.hops.hopsworks.expat.db.dao.project.ExpatProjectFacade;
import io.hops.hopsworks.expat.elastic.ElasticClient;
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
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.SQLException;

public class OpenSearchToRonDBMigration implements MigrateStep {
  private final static Logger LOGGER = LoggerFactory.getLogger(OpenSearchToRonDBMigration.class);

  protected ExpatModelsController expatModelsController;
  protected ExpatInodeController expatInodeController;
  protected ExpatProjectFacade expatProjectFacade;

  protected Connection connection;
  private CloseableHttpClient httpClient;
  private HttpHost elastic;
  private String elasticUser;
  private String elasticPass;

  private void setup()
    throws SQLException, ConfigurationException, GeneralSecurityException {
    connection = DbConnectionFactory.getConnection();
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
    httpClient = HttpClients
      .custom()
      .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.IGNORE_COOKIES).build())
      .setSSLContext(new SSLContextBuilder().loadTrustMaterial((x509Certificates, s) -> true).build())
      .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
      .build();
    this.expatModelsController = new ExpatModelsController(this.connection);
    this.expatInodeController = new ExpatInodeController(this.connection);
    this.expatProjectFacade = new ExpatProjectFacade(ExpatProject.class, this.connection);
  }

  private void close() throws SQLException, IOException {
    if(connection != null) {
      connection.close();
    }
    if(httpClient != null) {
      httpClient.close();
    }
  }

  @Override
  public void migrate() throws MigrationException {
    //TODO make sure elastic does not limit number of returned responses
    try {
      setup();
      LOGGER.info("Getting all file provenance indices");
      JSONArray fileProvIndices = ElasticClient.getIndicesByRegex(httpClient, elastic, elasticUser, elasticPass,
        "*__file_prov");
      LOGGER.info(fileProvIndices.toString());
      if (fileProvIndices.length() > 0) {
        LOGGER.info("Found {} file provenance indices to migrate", fileProvIndices.length());
        for(int i = 0; i < fileProvIndices.length(); i++) {
          JSONObject fileProvIndexObj = fileProvIndices.getJSONObject(i);
          String fileProvIndexName = fileProvIndexObj.getString("index");
          long projectInodeId = Long.parseLong(fileProvIndexName.substring(0, fileProvIndexName.indexOf("__")));

          ExpatHdfsInode projectInode = expatInodeController.getInodeById(projectInodeId);
          ExpatHdfsInode modelDatasetInode = expatInodeController.getInodeAtPath(
            String.format("/Projects/%s/Models", projectInode.getName()));

          String query = "{\"from\":0,\"size\":10000,\"query\":{\"bool\":{\"must\":[{\"term\":{\"entry_type\":" +
            "{\"value\":\"state\",\"boost\":1.0}}},{\"bool\":{\"should\":[{\"term\":{\"project_i_id\":" +
            "{\"value\":\"" + projectInode.getId() + "\",\"boost\":1.0}}}]" +
            ",\"adjust_pure_negative\":true,\"boost\":1.0}},{\"bool\":" +
            "{\"should\":[{\"term\":{\"ml_type\":{\"value\":\"MODEL\",\"boost\":1.0}}}]," +
            "\"adjust_pure_negative\":true,\"boost\":1.0}},{\"bool\":{\"should\":[{\"term\":{\"dataset_i_id\":" +
            "{\"value\":\"" + modelDatasetInode.getId() +"\",\"boost\":1.0}}}]" +
            ",\"adjust_pure_negative\":true,\"boost\":1.0}},{\"exists\":" +
            "{\"field\":\"xattr_prov.model_summary.value\",\"boost\":1.0}}]," +
            "\"adjust_pure_negative\":true,\"boost\":1.0}}}";

          JSONObject resp = ElasticClient.search(httpClient, elastic, elasticUser, elasticPass, fileProvIndexName,
            query);

          JSONArray modelHits = resp.getJSONObject("hits").getJSONArray("hits");
          if(modelHits.length() > 0) {
            LOGGER.info("Migrating {} model versions for project {}", modelHits.length(), projectInode.getName());
            for(int y = 0; y < modelHits.length(); y++) {
              JSONObject modelHit = modelHits.getJSONObject(y);
              JSONObject source = modelHit.getJSONObject("_source");
              JSONObject xattrProv = source.getJSONObject("xattr_prov");
              JSONObject modelSummary = xattrProv.getJSONObject("model_summary");
              JSONObject value = modelSummary.getJSONObject("value");
              String modelName = value.getString("name");

              ExpatProject expatProject = expatProjectFacade.findByProjectName(projectInode.getName());

              ExpatModel expatModel = expatModelsController.getByProjectAndName(expatProject.getId(), modelName);
              if(expatModel == null) {
                LOGGER.info("Could not find model ");
                expatModel = expatModelsController.insertModel(connection, modelName, expatProject.getId(),
                  false);
              }
              ExpatModelVersion expatModelVersion = expatModelsController.insertModelVersion(connection,
                expatModel.getId())
            }
          } else {
            LOGGER.info("Found no model versions to migrate for project {}", projectInode.getName());
          }
        }
      }
    } catch (SQLException | ConfigurationException | GeneralSecurityException | IOException | URISyntaxException |
             IllegalAccessException | InstantiationException e) {
      throw new MigrationException("error", e);
    } finally {
      try {
        close();
      } catch (SQLException | IOException e) {
        throw new MigrationException("error on close", e);
      }
    }
  }

  @Override
  public void rollback() throws RollbackException {
    //Empty model/model_version table
  }
}
