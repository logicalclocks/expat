/**
 * This file is part of Expat
 * Copyright (C) 2020, Logical Clocks AB. All rights reserved
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
package io.hops.hopsworks.expat.migrations.projects.search.featurestore;

import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.provenance.core.dto.ProvCoreDTO;
import io.hops.hopsworks.common.provenance.core.dto.ProvTypeDTO;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import io.hops.hopsworks.expat.migrations.projects.provenance.HopsClient;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.persistence.jaxb.JAXBContextFactory;
import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.eclipse.persistence.oxm.MediaType;
import org.elasticsearch.common.CheckedBiConsumer;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class UpdateFeaturegroupsForSearch implements MigrateStep {
  private final static Logger LOGGER = LogManager.getLogger(UpdateFeaturegroupsForSearch.class);
  
  private final static String GET_ALL_FEATURESTORES = "SELECT id, project_id FROM feature_store";
  private final static int GET_ALL_FEATURESTORES_S_ID = 1;
  private final static int GET_ALL_FEATURESTORES_S_PROJECT_ID = 2;
  private final static String GET_FEATUREGROUPS = "SELECT name, version, created, creator, cached_feature_group_id " +
    "FROM feature_group WHERE feature_store_id=? AND cached_feature_group_id is not null";
  private final static int GET_FEATUREGROUPS_W_FS_ID = 1;
  private final static int GET_FEATUREGROUPS_S_NAME = 1;
  private final static int GET_FEATUREGROUPS_S_VERSION = 2;
  private final static int GET_FEATUREGROUPS_S_CREATED = 3;
  private final static int GET_FEATUREGROUPS_S_CREATOR = 4;
  private final static int GET_FEATUREGROUPS_S_HIVE_TBL_ID = 5;
  private final static String GET_USER = "SELECT email FROM users WHERE uid=?";
  private final static int GET_USER_W_ID = 1;
  private final static int GET_USER_S_EMAIL = 1;
  private final static String GET_FG_DESCRIPTION = "SELECT PARAM_VALUE FROM metastore.TABLE_PARAMS " +
    "WHERE TBL_ID=? AND PARAM_KEY=?";
  private final static int GET_FG_DESCRIPTION_W_TBL_ID = 1;
  private final static int GET_FG_DESCRIPTION_W_PARAM = 2;
  private final static int GET_FG_DESCRIPTION_S_DESCRIPTION = 1;
  private final static String GET_FG_FEATURES = "SELECT c.COLUMN_NAME" +
    " FROM metastore.TBLS t JOIN metastore.SDS s JOIN metastore.COLUMNS_V2 c" +
    " ON t.SD_ID=s.SD_ID AND s.CD_ID=c.CD_ID WHERE t.TBL_ID = ?";
  private final static int GET_FG_FEATURES_W_TBL_ID = 1;
  private final static int GET_FG_FEATURES_S_NAME = 1;
  private final static String GET_PROJECT = "SELECT inode_name FROM project WHERE id=?";
  private final static int GET_PROJECT_W_ID = 1;
  private final static int GET_PROJECT_S_NAME = 1;
  private final static String GET_FEATUREGROUP_TYPE = "SELECT TBL_TYPE FROM metastore.TBLS WHERE TBL_ID=?";
  private final static int GET_FEATUREGROUP_TYPE_W_TBL_ID = 1;
  private final static int GET_FEATUREGROUP_TYPE_S_TYPE = 1;
  
  protected Connection connection = null;
  DistributedFileSystemOps dfso = null;
  private String hopsUser;
  SimpleDateFormat formatter;
  JAXBContext jaxbContext;
  
  private void setup() throws ConfigurationException, SQLException, JAXBException {
    formatter = new SimpleDateFormat("yyyy-M-dd hh:mm:ss", Locale.ENGLISH);
    jaxbContext = jaxbContext();
    
    connection = DbConnectionFactory.getConnection();
    
    Configuration conf = ConfigurationBuilder.getConfiguration();
    hopsUser = conf.getString(ExpatConf.HOPS_CLIENT_USER);
    if (hopsUser == null) {
      throw new ConfigurationException(ExpatConf.HOPS_CLIENT_USER + " cannot be null");
    }
    dfso = HopsClient.getDFSO(hopsUser);
  }
  
  private void close() throws SQLException {
    if(connection != null) {
      connection.close();
    }
    if(dfso != null) {
      dfso.close();
    }
  }
  
  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("featuregroup search migration");
    try {
      setup();
      traverseElements(migrateFeaturegroup());
    } catch (Exception e) {
      throw new MigrationException("error", e);
    } finally {
      try {
        close();
      } catch (SQLException e) {
        throw new MigrationException("error", e);
      }
    }
  }
  
  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("featuregroup search rollback");
    try {
      setup();
      traverseElements(revertFeaturegroup());
    } catch (Exception e) {
      throw new RollbackException("error", e);
    } finally {
      try {
        close();
      } catch (SQLException e) {
        throw new RollbackException("error", e);
      }
    }
  }
  
  private void traverseElements(CheckedBiConsumer<ResultSet, ResultSet, Exception> action)
    throws Exception {
    PreparedStatement allFeaturestoresStmt = null;
    PreparedStatement allFSFeaturegroupsStmt = null;
    PreparedStatement featuregroupTypeStmt = null;
    
    try {
      connection.setAutoCommit(false);
      allFeaturestoresStmt = connection.prepareStatement(GET_ALL_FEATURESTORES);
      ResultSet allFeaturestoresResultSet = allFeaturestoresStmt.executeQuery();
    
      while (allFeaturestoresResultSet.next()) {
        allFSFeaturegroupsStmt = getFSFeaturegroupsStmt(allFeaturestoresResultSet);
        ResultSet allFSFeaturegroupsResultSet = allFSFeaturegroupsStmt.executeQuery();
        while(allFSFeaturegroupsResultSet.next()) {
          featuregroupTypeStmt = getFeaturegroupType(allFSFeaturegroupsResultSet);
          ResultSet featuregroupTypeResultSet = featuregroupTypeStmt.executeQuery();
          if(featuregroupTypeResultSet.next()) {
            String featuregroupType = featuregroupTypeResultSet.getString(GET_FEATUREGROUP_TYPE_S_TYPE);
            if(featuregroupType.equals("MANAGED_TABLE")) {
              action.accept(allFeaturestoresResultSet, allFSFeaturegroupsResultSet);
            }
          } else {
            throw new IllegalStateException("featuregroup type not found");
          }
        }
        allFSFeaturegroupsStmt.close();
      }
      allFeaturestoresStmt.close();
      connection.commit();
      connection.setAutoCommit(true);
    } finally {
      if(allFSFeaturegroupsStmt != null) {
        allFSFeaturegroupsStmt.close();
      }
      if(allFeaturestoresStmt != null) {
        allFeaturestoresStmt.close();
      }
      close();
    }
  }
  
  private CheckedBiConsumer<ResultSet, ResultSet, Exception> migrateFeaturegroup() {
    return (ResultSet allFeaturestoresResultSet, ResultSet allFSFeaturegroupsResultSet) -> {
      String projectName = getProjectName(allFeaturestoresResultSet);
      String featuregroupName = allFSFeaturegroupsResultSet.getString(GET_FEATUREGROUPS_S_NAME);
      int featuregroupVersion = allFSFeaturegroupsResultSet.getInt(GET_FEATUREGROUPS_S_VERSION);
      String featuregroupPath = getFeaturegroupPath(projectName, featuregroupName, featuregroupVersion);
      LOGGER.info("featuregroup:{}", featuregroupPath);
      
      int featurestoreId = allFeaturestoresResultSet.getInt(GET_ALL_FEATURESTORES_S_ID);
      String description = getDescription(allFSFeaturegroupsResultSet);
      Date createDate = formatter.parse(allFSFeaturegroupsResultSet.getString(GET_FEATUREGROUPS_S_CREATED));
      String creator = getCreator(allFSFeaturegroupsResultSet);
      List<String> features = getFeatures(allFSFeaturegroupsResultSet);
      FeaturegroupXAttr.FullDTO xattr = new FeaturegroupXAttr.FullDTO(featurestoreId, description, createDate,
        creator, features);
      dfso.upsertXAttr(featuregroupPath, "provenance.featurestore", jaxbParser(jaxbContext, xattr).getBytes());
    };
  }
  
  private CheckedBiConsumer<ResultSet, ResultSet, Exception> revertFeaturegroup() {
    return (ResultSet allFeaturestoresResultSet, ResultSet allFSFeaturegroupsResultSet) -> {
      String projectName = getProjectName(allFeaturestoresResultSet);
      String featuregroupName = allFSFeaturegroupsResultSet.getString(GET_FEATUREGROUPS_S_NAME);
      int featuregroupVersion = allFSFeaturegroupsResultSet.getInt(GET_FEATUREGROUPS_S_VERSION);
      String featuregroupPath = getFeaturegroupPath(projectName, featuregroupName, featuregroupVersion);
      LOGGER.info("featuregroup:{}", featuregroupPath);
      
      try {
        dfso.removeXAttr(featuregroupPath, "provenance.featurestore");
      } catch(RemoteException ex) {
        if(ex.getMessage().startsWith("No matching attributes found for remove operation")) {
          //ignore
        } else {
          throw ex;
        }
      }
    };
  }
  
  private PreparedStatement getFSFeaturegroupsStmt(ResultSet allFeaturestoresResultSet) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(GET_FEATUREGROUPS);
    stmt.setInt(GET_FEATUREGROUPS_W_FS_ID, allFeaturestoresResultSet.getInt(GET_ALL_FEATURESTORES_S_ID));
    return stmt;
  }
  private PreparedStatement getFGUserStmt(ResultSet allFSFeaturegroupsResultSet) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(GET_USER);
    stmt.setInt(GET_USER_W_ID, allFSFeaturegroupsResultSet.getInt(GET_FEATUREGROUPS_S_CREATOR));
    return stmt;
  }
  private PreparedStatement getFGDescriptionStmt(ResultSet allFSFeaturegroupsResultSet) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(GET_FG_DESCRIPTION);
    stmt.setInt(GET_FG_DESCRIPTION_W_TBL_ID, allFSFeaturegroupsResultSet.getInt(GET_FEATUREGROUPS_S_HIVE_TBL_ID));
    stmt.setString(GET_FG_DESCRIPTION_W_PARAM, "comment");
    return stmt;
  }
  private PreparedStatement getFGFeaturesStmt(ResultSet allFSFeaturegroupsResultSet) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(GET_FG_FEATURES);
    stmt.setInt(GET_FG_FEATURES_W_TBL_ID, allFSFeaturegroupsResultSet.getInt(GET_FEATUREGROUPS_S_HIVE_TBL_ID));
    return stmt;
  }
  private PreparedStatement getProjectStmt(ResultSet allFeaturestoresResultSet) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(GET_PROJECT);
    stmt.setInt(GET_PROJECT_W_ID, allFeaturestoresResultSet.getInt(GET_ALL_FEATURESTORES_S_PROJECT_ID));
    return stmt;
  }
  private PreparedStatement getFeaturegroupType(ResultSet allFSFeaturegroupsResultSet) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(GET_FEATUREGROUP_TYPE);
    stmt.setInt(GET_FEATUREGROUP_TYPE_W_TBL_ID, allFSFeaturegroupsResultSet.getInt(GET_FEATUREGROUPS_S_HIVE_TBL_ID));
    return stmt;
  }
  
  private String getCreator(ResultSet allFSFeaturegroupsResultSet) throws SQLException {
    PreparedStatement fgUserStmt = null;
    try {
      fgUserStmt = getFGUserStmt(allFSFeaturegroupsResultSet);
      ResultSet fgUserResultSet = fgUserStmt.executeQuery();
      if (fgUserResultSet.next()) {
        return fgUserResultSet.getString(GET_USER_S_EMAIL);
      } else {
        throw new IllegalStateException("featuregroup creator not found");
      }
    } finally {
      if(fgUserStmt != null) {
        fgUserStmt.close();
      }
    }
  }
  
  private String getDescription(ResultSet allFSFeaturegroupsResultSet) throws SQLException {
    PreparedStatement fgDescriptionStmt = null;
    try {
      fgDescriptionStmt = getFGDescriptionStmt(allFSFeaturegroupsResultSet);
      ResultSet fgDescriptionResultSet = fgDescriptionStmt.executeQuery();
      if (fgDescriptionResultSet.next()) {
        return fgDescriptionResultSet.getString(GET_FG_DESCRIPTION_S_DESCRIPTION);
      } else {
        throw new IllegalStateException("featuregroup description not found");
      }
    } finally {
      if(fgDescriptionStmt != null) {
        fgDescriptionStmt.close();
      }
    }
  }
  
  private List<String> getFeatures(ResultSet allFSFeaturegroupsResultSet) throws SQLException {
    PreparedStatement fgFeaturesStmt = null;
    try {
      fgFeaturesStmt = getFGFeaturesStmt(allFSFeaturegroupsResultSet);
      ResultSet fgFeaturesResultSet = fgFeaturesStmt.executeQuery();
      List<String> features = new LinkedList<>();
      while (fgFeaturesResultSet.next()) {
        features.add(fgFeaturesResultSet.getString(GET_FG_FEATURES_S_NAME));
      }
      return features;
    } finally {
      if(fgFeaturesStmt != null) {
        fgFeaturesStmt.close();
      }
    }
  }
  
  private String getProjectName(ResultSet allFeaturestoreResultSet) throws SQLException {
    PreparedStatement projectStmt = null;
    try {
      projectStmt = getProjectStmt(allFeaturestoreResultSet);
      ResultSet projectsResultSet = projectStmt.executeQuery();
      if (projectsResultSet.next()) {
        return projectsResultSet.getString(GET_PROJECT_S_NAME);
      } else {
        throw new IllegalStateException("project parent not found");
      }
    } finally {
      if(projectStmt != null) {
        projectStmt.close();
      }
    }
  }
  
  private JAXBContext jaxbContext() throws JAXBException {
    Map<String, Object> properties = new HashMap<>();
    properties.put(MarshallerProperties.JSON_INCLUDE_ROOT, false);
    properties.put(MarshallerProperties.MEDIA_TYPE, MediaType.APPLICATION_JSON);
    JAXBContext context = JAXBContextFactory.createContext(
      new Class[] {
        ProvCoreDTO.class,
        ProvTypeDTO.class,
        FeaturegroupXAttr.FullDTO.class
      },
      properties);
    return context;
  }
  
  private String jaxbParser(JAXBContext jaxbContext, FeaturegroupXAttr.FullDTO xattr) throws JAXBException {
    Marshaller marshaller = jaxbContext.createMarshaller();
    StringWriter sw = new StringWriter();
    marshaller.marshal(xattr, sw);
    return sw.toString();
  }
  
  private String getFeaturegroupPath(String project, String featuregroup, int version) {
    return "/apps/hive/warehouse/" + project.toLowerCase() + "_featurestore.db/" + featuregroup + "_" + version;
  }
}
