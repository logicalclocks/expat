package io.hops.hopsworks.expat.migrations.airflow;

import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.hdfs.FsPermissions;
import io.hops.hopsworks.common.util.ProcessDescriptor;
import io.hops.hopsworks.common.util.ProcessResult;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.db.dao.hdfs.inode.ExpatInodeController;
import io.hops.hopsworks.expat.executor.ProcessExecutor;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import io.hops.hopsworks.expat.migrations.projects.util.HopsClient;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DagsMigration implements MigrateStep {
  private static final Logger LOGGER = LogManager.getLogger(DagsMigration.class);
  private static final String AIRFLOW_USER = "airflow";
  private static final String AIRFLOW_DATASET_NAME = "Airflow";
  public static final String README_TEMPLATE = "*This is an auto-generated README.md"
      + " file for your Dataset!*\n"
      + "To replace it, go into your DataSet and edit the README.md file.\n"
      + "\n" + "*%s* DataSet\n" + "===\n" + "\n"
      + "## %s";
  private static final String AIRFLOW_DATASET_DESCRIPTION = "Contains airflow dags";
  protected Connection connection;
  protected ExpatInodeController inodeController;
  DistributedFileSystemOps dfso = null;

  private String expatPath = null;
  private String hadoopHome = null;
  private String hopsClientUser = null;
  private boolean dryRun;



  private void setup() throws ConfigurationException, SQLException {
    connection = DbConnectionFactory.getConnection();
    Configuration config = ConfigurationBuilder.getConfiguration();
    expatPath = config.getString(ExpatConf.EXPAT_PATH);
    hopsClientUser = config.getString(ExpatConf.HOPS_CLIENT_USER);
    if (hopsClientUser == null) {
      throw new ConfigurationException(ExpatConf.HOPS_CLIENT_USER + " cannot be null");
    }
    dfso = HopsClient.getDFSO(hopsClientUser);
    hadoopHome = System.getenv("HADOOP_HOME");
    dryRun = config.getBoolean(ExpatConf.DRY_RUN);
    inodeController = new ExpatInodeController(connection);
  }

  @Override
  public void migrate() throws MigrationException {
    try {
      setup();
      Statement stmt = connection.createStatement();
      ResultSet resultSet = stmt.executeQuery("SELECT project.id, projectname,users.username FROM project " +
          "JOIN users ON project.username=users.email;");
      while (resultSet.next()) {
        String projectName = resultSet.getString("projectname");
        Integer projectId = resultSet.getInt("id");
        String username = resultSet.getString("username");
        String hdfsUsername = getHdfsUserName(username, projectName);
        String projectSecret = DigestUtils.sha256Hex(Integer.toString(projectId));
        createAirflowDataset(projectId, projectName, hdfsUsername);
        try {
          ProcessDescriptor dagEnvMigrateProc = new ProcessDescriptor.Builder()
              .addCommand(expatPath + "/bin/dags_migrate.sh")
              .addCommand(projectName)
              .addCommand(projectSecret)
              .addCommand(hdfsUsername)
              .addCommand(hopsClientUser)
              .addCommand(hadoopHome)
              .ignoreOutErrStreams(false)
              .setWaitTimeout(30, TimeUnit.MINUTES)
              .build();

          ProcessResult processResult = ProcessExecutor.getExecutor().execute(dagEnvMigrateProc);
          if (processResult.getExitCode() == 0) {
            LOGGER.info("Successfully moved dags for project: " + projectName);
          } else if (processResult.getExitCode() == 2) {
            LOGGER.info("Dags directory for project: " + projectName + ", was not configured. So it does " +
                "not have any dags.");
          } else {
            LOGGER.error("Failed to copy dags for project: " + projectName +
                " " + processResult.getStdout());
          }
        } catch (IOException e) {
          // Keep going
          LOGGER.error("Failed to copy dags for project: " + projectName + " " + e.getMessage());
        }
      }
    } catch (Exception ex) {
      throw new MigrationException("Error in migration step " + DagsMigration.class.getSimpleName(), ex);
    } finally {
      close();
    }
  }


  private void createAirflowDataset(Integer projectId, String projectName, String hdfsUsername)
      throws IOException, SQLException, MigrationException {
    Path airflowDatasetPath = new Path( "hdfs:///Projects/" + projectName + "/"  + AIRFLOW_DATASET_NAME);
    boolean exists = dfso.exists(airflowDatasetPath);
    if (!dfso.exists(airflowDatasetPath) && !dryRun) {
      LOGGER.info("Creating dataset Airflow in " + projectName);
      FsPermission fsPermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE, false);
      dfso.mkdir(airflowDatasetPath, fsPermission);
      createAirflowDataset(projectId, airflowDatasetPath);
      String datasetGroup = getAirflowDatasetGroup(projectName);
      dfso.setOwner(airflowDatasetPath, hdfsUsername, datasetGroup);
      String datasetAclGroup = getAirflowDatasetAclGroup(projectName);
      addGroup(datasetAclGroup);
      addUserToGroup(hdfsUsername, datasetAclGroup);
      dfso.setPermission(airflowDatasetPath, getDefaultDatasetAcl(datasetAclGroup));
      addProjectMembersToAirflowDataset(projectId, projectName);
      // set the airflow acls
      dfso.getFilesystem().modifyAclEntries(airflowDatasetPath, getAirflowAcls());
      createAirflowDatasetReadme(hdfsUsername, projectName, airflowDatasetPath);
    } else {
      LOGGER.info("Airflow dataset already exist for project: " + projectName);
    }
  }

  private void createAirflowDataset(Integer projectId, Path airflowDatasetPath) throws MigrationException,
      SQLException {
    // check if it does not already exist
    Statement stmt = connection.createStatement();
    ResultSet resultSet = stmt.executeQuery("SELECT id FROM dataset WHERE projectId=" + projectId
        + " AND inode_name='" + AIRFLOW_DATASET_NAME   + "';");
    if (!resultSet.next()) {
      connection.setAutoCommit(false);
      PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO " +
          "dataset (inode_name, projectId, description, searchable, permission) VALUES (?, ? ,?, ?, ?)");
      preparedStatement.setString(1, AIRFLOW_DATASET_NAME);
      preparedStatement.setInt(2, projectId);
      preparedStatement.setString(3, AIRFLOW_DATASET_DESCRIPTION);
      preparedStatement.setInt(4, 1);
      preparedStatement.setString(5, "EDITABLE");
      preparedStatement.execute();
      preparedStatement.close();
      connection.commit();
      connection.setAutoCommit(true);
    }
  }

  private void createAirflowDatasetReadme(String hdfsUsername, String projectName, Path datasetPath) {
    Path readMeFilePath = new Path(datasetPath, "README.md");
    String readmeFile = String.format(README_TEMPLATE, AIRFLOW_DATASET_NAME, AIRFLOW_DATASET_DESCRIPTION);
    try (FSDataOutputStream fsOut = dfso.create(readMeFilePath)) {
      fsOut.writeBytes(readmeFile);
      fsOut.flush();
      dfso.setPermission(readMeFilePath, FsPermissions.rwxrwx___);
      dfso.setOwner(readMeFilePath, hdfsUsername, getAirflowDatasetGroup(projectName));
    } catch (IOException ex) {
      LOGGER.info("Failed to create README for project " + projectName, ex.getMessage());
    }
  }

  private void addProjectMembersToAirflowDataset(Integer projectId, String projectName)
      throws SQLException, IOException {
    // Get the members
    Statement stmt = connection.createStatement();
    ResultSet resultSet = stmt.executeQuery("SELECT  username, team_role FROM users JOIN project_team WHERE u" +
        "sers.email = project_team.team_member AND project_id=" + projectId);
    while (resultSet.next()) {
      String username = resultSet.getString("username");
      String hdfsUserName = getHdfsUserName(username, projectName);
      addUserToGroup(hdfsUserName, getAirflowDatasetGroup(projectName));
    }
  }

  private void addGroup(String group) throws IOException {
    try {
      dfso.addGroup(group);
    } catch (IOException e) {
      if (e.getMessage().contains(group + " already exists")) {
        // continue
        LOGGER.info("Group " + group + " already exist");
      } else {
        throw e;
      }
    }
  }

  private void addUserToGroup(String username, String group) throws IOException {
    try {
      dfso.addUserToGroup(username, group);
    } catch (IOException e) {
      if (e.getMessage().contains(username + " is already part of Group: " + group)) {
        // continue
        LOGGER.info(username + " is already part of Group: " + group);
      } else {
        throw e;
      }
    }
  }

  private String getHdfsUserName(String username, String projectName) {
    return projectName + "__" + username;
  }

  private String getAirflowDatasetGroup(String projectName) {
    return projectName + "__" + AIRFLOW_DATASET_NAME;
  }

  private String getAirflowDatasetAclGroup(String projectName) {
    return getAirflowDatasetGroup(projectName) + "__read";
  }

  private List<AclEntry> getAirflowAcls() {
    List<AclEntry> aclEntries = new ArrayList<>();
    AclEntry accessAcl = new AclEntry.Builder()
        .setType(AclEntryType.USER)
        .setName(AIRFLOW_USER)
        .setScope(AclEntryScope.ACCESS)
        .setPermission(FsAction.READ_EXECUTE)
        .build();
    AclEntry defaultAcl = new AclEntry.Builder()
        .setType(AclEntryType.USER)
        .setName(AIRFLOW_USER)
        .setScope(AclEntryScope.DEFAULT)
        .setPermission(FsAction.READ_EXECUTE)
        .build();
    aclEntries.add(accessAcl);
    aclEntries.add(defaultAcl);
    return aclEntries;
  }

  private List<AclEntry> getDefaultDatasetAcl(String aclGroup) {
    List<AclEntry> aclEntries = new ArrayList<>();
    AclEntry aclEntryUser = new AclEntry.Builder()
        .setType(AclEntryType.USER)
        .setScope(AclEntryScope.ACCESS)
        .setPermission(FsAction.ALL)
        .build();
    aclEntries.add(aclEntryUser);
    AclEntry aclEntryGroup = new AclEntry.Builder()
        .setType(AclEntryType.GROUP)
        .setScope(AclEntryScope.ACCESS)
        .setPermission(FsAction.ALL)
        .build();
    aclEntries.add(aclEntryGroup);
    AclEntry aclEntryDatasetGroup = new AclEntry.Builder()
        .setType(AclEntryType.GROUP)
        .setName(aclGroup)
        .setScope(AclEntryScope.ACCESS)
        .setPermission(FsAction.READ_EXECUTE)
        .build();
    aclEntries.add(aclEntryDatasetGroup);
    AclEntry aclEntryOther = new AclEntry.Builder()
        .setType(AclEntryType.OTHER)
        .setScope(AclEntryScope.ACCESS)
        .setPermission(FsAction.NONE)
        .build();
    aclEntries.add(aclEntryOther);
    AclEntry aclEntryDefault = new AclEntry.Builder()
        .setType(AclEntryType.GROUP)
        .setName(aclGroup)
        .setScope(AclEntryScope.DEFAULT)
        .setPermission(FsAction.READ_EXECUTE)
        .build();
    aclEntries.add(aclEntryDefault);
    return aclEntries;
  }

  protected void close() {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException ex) {
        LOGGER.error("failed to close jdbc connection", ex);
      }
    }
    if(dfso != null) {
      dfso.close();
    }
  }

  @Override
  public void rollback() throws RollbackException {

  }
}
