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
package io.hops.hopsworks.expat.epipe;

import io.hops.hopsworks.expat.elastic.ElasticClient;
import org.apache.http.HttpHost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class EpipeRunner {
  private static final Logger LOGGER = LogManager.getLogger(EpipeRunner.class);
  
  private static final String PROJECTS_CONFIG_LINE = "index = projects";
  private static final String FEATURESTORE_CONFIG_LINE = "featurestore_index = featurestore";
  private static final String APP_PROVENANCE_CONFIG_LINE = "app_provenance_index = appprovenance";
  
  private static void fixReindexOf(String elasticIndex, List<String> toReindex,
                                   List<String> configContent, int configContentIdx, String configLine) {
    if (configContent.get(configContentIdx).startsWith(configLine)) {
      if(toReindex.contains(elasticIndex)) {
        toReindex.remove(elasticIndex);
      } else {
        //comment out
        configContent.set(configContentIdx, "#" + configLine);
      }
    } else if (configContent.get(configContentIdx).startsWith("#" + configLine)) {
      if(toReindex.contains(elasticIndex)) {
        toReindex.remove(elasticIndex);
        //uncomment
        configContent.set(configContentIdx, configLine);
      }
    }
  }
  
  private static void updateReindexConfig(String reindexConfigPath, List<String> toReindex) throws IOException {
    Path configFile = Paths.get(reindexConfigPath);
    List<String> configContent = new ArrayList<>(Files.readAllLines(configFile, StandardCharsets.UTF_8));
  
    List<String> toReindexAux = new LinkedList<>(toReindex);
    for (int i = 0; i < configContent.size(); i++) {
      fixReindexOf("projects", toReindexAux, configContent, i, PROJECTS_CONFIG_LINE);
      fixReindexOf("featurestore", toReindex, configContent, i, FEATURESTORE_CONFIG_LINE);
      fixReindexOf("app_provenance", toReindex, configContent, i, APP_PROVENANCE_CONFIG_LINE);
    }
  
    Files.write(configFile, configContent, StandardCharsets.UTF_8);
  }
  
  public static void stopEpipe() throws IOException, InterruptedException {
    LOGGER.info("stopping epipe");
    Process stopEpipe = Runtime.getRuntime().exec("systemctl stop epipe");
    BufferedReader stopEpipeReader = new BufferedReader(new InputStreamReader(stopEpipe.getInputStream()));
    String line = null;
    while ( (line = stopEpipeReader.readLine()) != null) {
      LOGGER.info(line);
    }
    stopEpipe.waitFor();
    Thread.sleep(1000);
  }
  
  public static void reindex(CloseableHttpClient httpClient, HttpHost elastic, String elasticUser, String elasticPass,
                             String epipePath, List<String> toReindex)
    throws IOException, InterruptedException {
    LOGGER.info("delete indices");
    for(String index : toReindex) {
      ElasticClient.deleteIndex(httpClient, elastic, elasticUser, elasticPass, index);
    }
  
    LOGGER.info("create indices");
    for(String index : toReindex) {
      ElasticClient.createIndex(httpClient, elastic, elasticUser, elasticPass, index);
    }
  
    LOGGER.info("reindex config");
    String reindexConfigPath = epipePath + "/conf/config-reindex.ini";
    updateReindexConfig(reindexConfigPath, toReindex);
  
    LOGGER.info("reindex");
    ProcessBuilder epipeReindexB = new ProcessBuilder()
      .inheritIO()
      .redirectErrorStream(true)
      .redirectOutput(new File(epipePath + "/epipe-reindex.log"))
      .command(epipePath + "/bin/epipe", "-c", reindexConfigPath);
  
    Map<String, String> env = epipeReindexB.environment();
    env.put("LD_LIBRARY_PATH", "/srv/hops/mysql/lib:${LD_LIBRARY_PATH}");
  
    Process epipeReindex = epipeReindexB.start();
    epipeReindex.waitFor();
    LOGGER.info("reindex completed");
  }
  
  public static void restartEpipe() throws IOException, InterruptedException {
    LOGGER.info("restart epipe");
    Process restartEpipe = Runtime.getRuntime().exec("systemctl restart epipe");
    BufferedReader restartEpipeReader = new BufferedReader(new InputStreamReader(restartEpipe.getInputStream()));
    String line = null;
    while ( (line = restartEpipeReader.readLine()) != null) {
      LOGGER.info(line);
    }
    restartEpipe.waitFor();
  }
  
  public static void run(CloseableHttpClient httpClient, HttpHost elastic, String elasticUser, String elasticPass,
                         String epipePath, List<String> toReindex)
    throws IOException, InterruptedException {
    stopEpipe();
    reindex(httpClient, elastic, elasticUser, elasticPass, epipePath, toReindex);
    restartEpipe();
  }
}
