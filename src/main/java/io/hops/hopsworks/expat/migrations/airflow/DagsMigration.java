package io.hops.hopsworks.expat.migrations.airflow;

import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.common.util.ProcessDescriptor;
import io.hops.hopsworks.common.util.ProcessResult;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.executor.ProcessExecutor;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import io.hops.hopsworks.expat.migrations.projects.util.HopsClient;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DagsMigration implements MigrateStep {
  private static final Logger LOGGER = LogManager.getLogger(DagsMigration.class);
  protected Connection connection;
  protected DistributedFileSystemOps dfso = null;

  private String expatPath = null;
  private String hadoopHome = null;
  private String hopsClientUser = null;

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
        String hdfsUsername = projectName + "__" + username;
        String projectSecret = DigestUtils.sha256Hex(Integer.toString(projectId));
        LOGGER.info("Project secret is " + projectSecret);
        try {
          createAirflowDataset(projectName, hdfsUsername);
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


  private void createAirflowDataset(String projectName, String hdfsUsername) throws IOException {
    Path projectAirflowDirectory = new Path("/Projects/" + projectName  + "/Airflow");
    if (!dfso.exists(projectAirflowDirectory)) {
      FsPermission fsPermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE, false);
      dfso.mkdir(projectAirflowDirectory, fsPermission);
      String group = projectName + "__Airflow";
      dfso.setOwner(projectAirflowDirectory,hdfsUsername, group);
      // set the airflow acls
      final String airflowUser = "airflow";
      List<AclEntry> aclEntries = new ArrayList<>();
      AclEntry accessAcl = new AclEntry.Builder()
          .setType(AclEntryType.USER)
          .setName(airflowUser)
          .setScope(AclEntryScope.ACCESS)
          .setPermission(FsAction.READ_EXECUTE)
          .build();
      AclEntry defaultAcl = new AclEntry.Builder()
          .setType(AclEntryType.USER)
          .setName(airflowUser)
          .setScope(AclEntryScope.DEFAULT)
          .setPermission(FsAction.READ_EXECUTE)
          .build();
      aclEntries.add(accessAcl);
      aclEntries.add(defaultAcl);
      dfso.getFilesystem().modifyAclEntries(projectAirflowDirectory, aclEntries);
    }
  }

  protected void close() {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException ex) {
        LOGGER.error("failed to close jdbc connection", ex);
      }
    }
    if (dfso != null) {
      dfso.close();
    }
  }

  @Override
  public void rollback() throws RollbackException {

  }
}
