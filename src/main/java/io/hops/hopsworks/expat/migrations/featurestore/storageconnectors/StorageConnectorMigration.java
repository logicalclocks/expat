package io.hops.hopsworks.expat.migrations.featurestore.storageconnectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.exceptions.FeaturestoreException;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import io.hops.hopsworks.expat.migrations.projects.util.HopsClient;
import io.hops.hopsworks.restutils.RESTCodes;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.stream.Collectors;

public class StorageConnectorMigration implements MigrateStep {
  private static final Logger LOGGER = LogManager.getLogger(StorageConnectorMigration.class);
  
  protected Connection connection;
  DistributedFileSystemOps dfso = null;
  private boolean dryRun;
  private String hopsUser;
  
  private ObjectMapper objectMapper = new ObjectMapper();
  
  private final static String STORAGE_CONNECTORS_RESOURCE_SUBDIR= "storage_connector_resources";
  private final static String FEATURESTORE_HIVE_DB_DIR = "hdfs:///apps/hive/warehouse/%s_featurestore.db";
  
  private final static String GET_ALL_SNOWFLAKE_CONNECTORS =
    "SELECT id, arguments FROM feature_store_snowflake_connector";
  private final static String UPDATE_SNOWFLAKE_ARGUMENTS =
    "UPDATE feature_store_snowflake_connector SET arguments = ? WHERE id = ?";
  private final static String GET_PROJECT_NAMES = "SELECT projectname FROM project";
  
  private void setup() throws ConfigurationException, SQLException {
    connection = DbConnectionFactory.getConnection();
    
    Configuration conf = ConfigurationBuilder.getConfiguration();
    hopsUser = conf.getString(ExpatConf.HOPS_CLIENT_USER);
    if (hopsUser == null) {
      throw new ConfigurationException(ExpatConf.HOPS_CLIENT_USER + " cannot be null");
    }
    dfso = HopsClient.getDFSO(hopsUser);
    dryRun = conf.getBoolean(ExpatConf.DRY_RUN);
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
    LOGGER.info("Starting storage connector migration");
    
    try {
      setup();
    } catch (ConfigurationException | SQLException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    } finally {
      try {
        close();
      } catch (SQLException e) {
        throw new MigrationException("error", e);
      }
    }
    
    migrateSnowflakeOptions();
    migrateConnectorResourcesDirectory();
    
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        throw new MigrationException("failed to close jdbc connection", e);
      }
    }
  
    LOGGER.info("Finished storage connector migration");
  }
  
  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("Starting storage connector rollback");
    try {
      setup();
    } catch (ConfigurationException | SQLException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      throw new RollbackException(errorMsg, ex);
    } finally {
      try {
        close();
      } catch (SQLException e) {
        throw new RollbackException("error", e);
      }
    }
  
    rollbackSnowflakeOptions();
    rollbackConnectorResourcesDirectory();
  
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        throw new RollbackException("failed to close jdbc connection", e);
      }
    }
    LOGGER.info("Finished storage connector rollback");
  }
  
  private void migrateSnowflakeOptions() throws MigrationException {
    LOGGER.info("Starting to migrate Snowflake Connector Options");
  
    try {
      connection.setAutoCommit(false);
    
      PreparedStatement getStatement = connection.prepareStatement(GET_ALL_SNOWFLAKE_CONNECTORS);
      PreparedStatement updateStatement = connection.prepareStatement(UPDATE_SNOWFLAKE_ARGUMENTS);
      ResultSet snowflakeConnectorArguments = getStatement.executeQuery();
    
      int currentConnectorId;
      String currentArguments;
      List<OptionDTO> arguments;
    
      while (snowflakeConnectorArguments.next()) {
        currentArguments = snowflakeConnectorArguments.getString("arguments");
        currentConnectorId = snowflakeConnectorArguments.getInt("id");
        
        try {
          // for idempotency try first to deserialize with new format
          arguments = toOptions(currentArguments);
        } catch (FeaturestoreException e) {
          arguments = oldToOptions(currentArguments);
        }
      
        if (!dryRun) {
          updateStatement.setString(1, fromOptions(arguments));
          updateStatement.setInt(2, currentConnectorId);
          updateStatement.execute();
        }
      }
      getStatement.close();
      updateStatement.close();
      connection.commit();
    
      connection.setAutoCommit(true);
    } catch (SQLException e) {
      throw new MigrationException("error", e);
    } finally {
      try {
        close();
      } catch (SQLException e) {
        throw new MigrationException("error", e);
      }
    }
    LOGGER.info("Finished to migrate Snowflake Connector Options");
  }
  
  private void migrateConnectorResourcesDirectory() throws MigrationException {
    LOGGER.info("Starting to migrate connector resources directory");
  
    try {
      connection.setAutoCommit(false);

      PreparedStatement projectNamesStatement = connection.prepareStatement(GET_PROJECT_NAMES);
      ResultSet projectNamesResultSet = projectNamesStatement.executeQuery();

      String currentProjectName;

      while (projectNamesResultSet.next()) {
        // check if project is feature store enabled by checking if the feature store hive db exists
        currentProjectName = projectNamesResultSet.getString("projectname");
        Path featureStorePath = new Path(String.format(FEATURESTORE_HIVE_DB_DIR, currentProjectName));
        if (dfso.exists(featureStorePath)) {
          FileStatus fileStatus = dfso.getFileStatus(featureStorePath);
          FsPermission featureStoreDbPermissions = fileStatus.getPermission();
          String owner = fileStatus.getOwner();
          String group = fileStatus.getGroup();
          Path storageConnectorResourcePath = new Path(featureStorePath + "/" + STORAGE_CONNECTORS_RESOURCE_SUBDIR);
          if (!dryRun && !dfso.exists(storageConnectorResourcePath)) {
            dfso.mkdir(storageConnectorResourcePath, featureStoreDbPermissions);
            dfso.setOwner(storageConnectorResourcePath, owner, group);
          }
        }
      }

      connection.commit();
      connection.setAutoCommit(true);
    } catch (SQLException | IOException e) {
      throw new MigrationException("error", e);
    } finally {
      try {
        close();
      } catch (SQLException e) {
        throw new MigrationException("error", e);
      }
    }
    
    LOGGER.info("Finished to migrate connector resources directory");
  }
  
  private void rollbackSnowflakeOptions() throws RollbackException {
    LOGGER.info("Starting to rollback Snowflake Connector Options");
    
    try {
      connection.setAutoCommit(false);
      
      PreparedStatement getStatement = connection.prepareStatement(GET_ALL_SNOWFLAKE_CONNECTORS);
      PreparedStatement updateStatement = connection.prepareStatement(UPDATE_SNOWFLAKE_ARGUMENTS);
      ResultSet snowflakeConnectorArguments = getStatement.executeQuery();
      
      int currentConnectorId;
      String currentArguments;
      List<OptionDTO> arguments;
      
      while (snowflakeConnectorArguments.next()) {
        currentArguments = snowflakeConnectorArguments.getString("arguments");
        currentConnectorId = snowflakeConnectorArguments.getInt("id");
        
        try {
          // for idempotency try first to deserialize with new format
          arguments = toOptions(currentArguments);
        } catch (FeaturestoreException e) {
          arguments = oldToOptions(currentArguments);
        }
        
        if (!dryRun) {
          updateStatement.setString(1, oldFromOptions(arguments));
          updateStatement.setInt(2, currentConnectorId);
          updateStatement.execute();
        }
      }
      getStatement.close();
      updateStatement.close();
      connection.commit();
      
      connection.setAutoCommit(true);
    } catch (SQLException e) {
      throw new RollbackException("error", e);
    } finally {
      try {
        close();
      } catch (SQLException e) {
        throw new RollbackException("error", e);
      }
    }
    LOGGER.info("Finished to rollback Snowflake Connector Options");
  }
  
  private void rollbackConnectorResourcesDirectory() throws RollbackException {
    LOGGER.info("Starting to rollback connector resources directory");
    
    try {
      connection.setAutoCommit(false);
      
      PreparedStatement projectNamesStatement = connection.prepareStatement(GET_PROJECT_NAMES);
      ResultSet projectNamesResultSet = projectNamesStatement.executeQuery();
      
      String currentProjectName;
      
      while (projectNamesResultSet.next()) {
        // check if project is feature store enabled by checking if the feature store hive db exists
        currentProjectName = projectNamesResultSet.getString("projectname");
        Path featureStorePath = new Path(String.format(FEATURESTORE_HIVE_DB_DIR, currentProjectName));
        if (dfso.exists(featureStorePath)) {
          Path storageConnectorResourcePath = new Path(featureStorePath + "/" + STORAGE_CONNECTORS_RESOURCE_SUBDIR);
          if (!dryRun && dfso.exists(storageConnectorResourcePath)) {
            dfso.rm(storageConnectorResourcePath, true);
          }
        }
      }
      
      connection.commit();
      connection.setAutoCommit(true);
    } catch (SQLException | IOException e) {
      throw new RollbackException("error", e);
    } finally {
      try {
        close();
      } catch (SQLException e) {
        throw new RollbackException("error", e);
      }
    }
    
    LOGGER.info("Finished to rollback connector resources directory");
  }
  
  private List<OptionDTO> oldToOptions(String arguments) {
    if (Strings.isNullOrEmpty(arguments)) {
      return null;
    }
    return Arrays.stream(arguments.split(";"))
      .map(arg -> arg.split("="))
      .map(a -> new OptionDTO(a[0], a[1]))
      .collect(Collectors.toList());
  }
  
  private String oldFromOptions(List<OptionDTO> options) {
    if (options == null || options.isEmpty()) {
      return null;
    }
    StringBuilder arguments = new StringBuilder();
    for (OptionDTO option : options) {
      arguments.append(arguments.length() > 0? ";" : "")
        .append(option.getName())
        .append("=")
        .append(option.getValue());
    }
    return arguments.toString();
  }
  
  // new toOptions
  public List<OptionDTO> toOptions(String arguments) throws FeaturestoreException {
    if (Strings.isNullOrEmpty(arguments)) {
      return null;
    }
    
    try {
      OptionDTO[] optionArray = objectMapper.readValue(arguments, OptionDTO[].class);
      return Arrays.asList(optionArray);
    } catch (JsonProcessingException e) {
      throw new FeaturestoreException(RESTCodes.FeaturestoreErrorCode.STORAGE_CONNECTOR_GET_ERROR, Level.SEVERE,
        "Error deserializing options list provided with connector", e.getMessage());
    }
  }
  
  // new fromOptions
  public String fromOptions(List<OptionDTO> options) {
    if (options == null || options.isEmpty()) {
      return null;
    }
    return new JSONArray(options).toString();
  }
}