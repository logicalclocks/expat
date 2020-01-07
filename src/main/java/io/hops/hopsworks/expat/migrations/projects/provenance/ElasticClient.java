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
package io.hops.hopsworks.expat.migrations.projects.provenance;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Base64;

public class ElasticClient {
  private final static Logger LOGGER = LogManager.getLogger(ElasticClient.class);
  
  public static void deleteProvenanceProjectIndex(CloseableHttpClient httpClient, HttpHost elastic, Long projectIId,
    String elasticUser, String elasticPass)
    throws IOException {
    String index = projectIId + "__file_prov";
    CloseableHttpResponse response = null;
    try {
      HttpDelete request = new HttpDelete(index);
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      String encodedAuth = Base64.getEncoder().encodeToString((elasticUser + ":" + elasticPass).getBytes());
      request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
      LOGGER.debug("Deleting index:{}", index);
      response = httpClient.execute(elastic, request);
      JSONObject jsonResponse = new JSONObject(EntityUtils.toString(response.getEntity()));
      int status = response.getStatusLine().getStatusCode();
      if (status == 200) {
        LOGGER.debug("Deleted index:{}", index);
      } else {
        if(!jsonResponse.getJSONObject("error").get("reason").toString().startsWith("no such index")) {
          throw new IllegalStateException("Could not delete index:" + index);
        }
        LOGGER.debug("Skipping index:{} - already deleted", index);
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
  
  public static void deleteAppProvenanceIndex(CloseableHttpClient httpClient, HttpHost elastic,
    String elasticUser, String elasticPass)
    throws IOException {
    String index = "app_prov";
    CloseableHttpResponse response = null;
    try {
      HttpDelete request = new HttpDelete(index);
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      String encodedAuth = Base64.getEncoder().encodeToString((elasticUser + ":" + elasticPass).getBytes());
      request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encodedAuth);
      LOGGER.debug("Deleting index:{}", index);
      response = httpClient.execute(elastic, request);
      JSONObject jsonResponse = new JSONObject(EntityUtils.toString(response.getEntity()));
      int status = response.getStatusLine().getStatusCode();
      if (status == 200) {
        LOGGER.debug("Deleted index:{}", index);
      } else {
        if (!jsonResponse.getJSONObject("error").get("reason").toString().startsWith("no such index")) {
          throw new IllegalStateException("Could not delete index:" + index);
        }
        LOGGER.debug("Skipping index:{} - already deleted", index);
      }
    } finally {
      if (response != null) {
        response.close();
      }
    }
  }
}
