/**
 * This file is part of Expat
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
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
package io.hops.hopsworks.expat.migrations.conda;

import io.hops.hopsworks.common.util.Settings;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

public class CreateKagentLogsIndeces implements MigrateStep {
  private static final Logger LOGGER = LogManager.getLogger(CreateKagentLogsIndeces.class);
  private static final String GET_CONDA_ENABLED_PROJECTS = "SELECT * FROM project WHERE conda_env = 1";
  
  private Connection connection;
  private static PoolingHttpClientConnectionManager httpConnectionManager;
  private CloseableHttpClient httpClient;
  private HttpHost kibana;
  
  private void setup() throws ConfigurationException, SQLException {
    connection = DbConnectionFactory.getConnection();
    Configuration conf = ConfigurationBuilder.getConfiguration();
    String kibanaURI = conf.getString(ExpatConf.KIBANA_URI);
    if (kibanaURI == null) {
      throw new ConfigurationException(ExpatConf.KIBANA_URI + " cannot be null");
    }
    kibana = HttpHost.create(kibanaURI);
    httpConnectionManager = new PoolingHttpClientConnectionManager();
    httpConnectionManager.setDefaultMaxPerRoute(5);
    httpClient = HttpClients.custom().setConnectionManager(httpConnectionManager).build();
  }
  
  @Override
  public void migrate() throws MigrationException {
    try {
      setup();
      // Get all Conda enabled projects
      Set<String> projects = getProjectsWithConda();
      
      // Create Kibana index template
      for (String projectName : projects) {
        try {
          createKibanaIndexPattern(projectName);
        } catch (IOException ex) {
          LOGGER.error("Ooops could not create index-pattern for " + projectName + " Moving on...", ex);
        }
      }
    } catch (Exception ex) {
      LOGGER.error(ex.getMessage(), ex);
      throw new MigrationException("Error in migration step " + CreateKagentLogsIndeces.class.getSimpleName(), ex);
    }
  }
  
  @Override
  public void rollback() throws RollbackException {
    try {
      setup();
      Set<String> projects = getProjectsWithConda();
      
      for (String projectName : projects) {
        try {
          deleteKibanaIndexPattern(projectName);
        } catch (IOException ex) {
          LOGGER.error("Ooops could not delete index-pattern for " + projectName + " Moving on...", ex);
        }
      }
    } catch (Exception ex) {
      LOGGER.error(ex.getMessage(), ex);
      throw new RollbackException("Error in rollback step " + CreateKagentLogsIndeces.class.getSimpleName(), ex);
    }
  }
  
  private Set<String> getProjectsWithConda() throws SQLException {
    Set<String> projects = new HashSet<>();
    ResultSet projectsRS = null;
    Statement projectsStmt = connection.createStatement();
    try {
      projectsRS = projectsStmt.executeQuery(GET_CONDA_ENABLED_PROJECTS);
      
      while (projectsRS.next()) {
        String projectName = projectsRS.getString("projectname");
        if (projectName != null && !projectName.isEmpty()) {
          LOGGER.debug("Found project " + projectName);
          projects.add(projectName.toLowerCase());
        }
      }
      return projects;
    } finally {
      if (projectsRS != null) {
        projectsRS.close();
      }
      if (projectsStmt != null) {
        projectsStmt.close();
      }
    }
  }
  
  private void createKibanaIndexPattern(String projectName) throws URISyntaxException, IOException {
    String indexPattern = projectName + Settings.ELASTIC_KAGENT_INDEX_PATTERN;
    String createIndexPatternStr = "/api/saved_objects/index-pattern/" + indexPattern;
    String payload = "{\"attributes\": {\"title\": \"" + indexPattern + "\"}}";
    
    CloseableHttpResponse response = null;
    try {
      HttpPost request = new HttpPost(createIndexPatternStr);
      request.setEntity(new StringEntity(payload));
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      request.addHeader("kbn-xsrf", "required");
      
      LOGGER.info("Creating index pattern: " + indexPattern);
      response = httpClient.execute(kibana, request);
      String responseStr = EntityUtils.toString(response.getEntity());
      int status = response.getStatusLine().getStatusCode();
      if (status == 200) {
        LOGGER.info("Created index-pattern: " + indexPattern);
      } else if (status == 409) {
        JSONObject jsonResponse = new JSONObject(responseStr);
        if (jsonResponse.getString("error").equals("Conflict")) {
          LOGGER.info("index-pattern " + indexPattern + " already exists");
        }
      }
      LOGGER.debug(responseStr);
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  private void deleteKibanaIndexPattern(String projectName) throws URISyntaxException, IOException {
    String indexPattern = projectName + Settings.ELASTIC_KAGENT_INDEX_PATTERN;
    String deleteIndexPatternPath = "/api/saved_objects/index-pattern/" + indexPattern;
    
    CloseableHttpResponse response = null;
    try {
      HttpDelete request = new HttpDelete(deleteIndexPatternPath);
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      request.addHeader("kbn-xsrf", "required");
      
      LOGGER.info("Deleting index pattern: " + indexPattern);
      response = httpClient.execute(kibana, request);
      int status = response.getStatusLine().getStatusCode();
      LOGGER.info("Return status: " + status);
      if (status == 200) {
        LOGGER.info("Deleted index pattern: " + indexPattern);
      } else {
        LOGGER.info("Could not delete index pattern " + indexPattern + " !!!");
      }
      if (LOGGER.isDebugEnabled()) {
        String responseStr = EntityUtils.toString(response.getEntity());
        LOGGER.debug(responseStr);
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  public static class ShutdownHook implements Runnable {
  
    @Override
    public void run() {
      if (httpConnectionManager != null) {
        httpConnectionManager.close();
      }
    }
  }
  
}
