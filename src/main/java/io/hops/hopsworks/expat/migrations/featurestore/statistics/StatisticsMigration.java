package io.hops.hopsworks.expat.migrations.featurestore.statistics;

import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.db.dao.hdfs.inode.ExpatInodeController;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import io.hops.hopsworks.expat.migrations.projects.util.HopsClient;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.rowset.serial.SerialBlob;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class StatisticsMigration implements MigrateStep {
  
  protected static final Logger LOGGER = LoggerFactory.getLogger(StatisticsMigration.class);
  
  protected Connection connection;
  protected DistributedFileSystemOps dfso = null;
  protected boolean dryRun;
  protected String hopsUser;
  
  protected ExpatInodeController inodeController;
  
  private static final String FEATURE_DESCRIPTIVE_STATISTICS_TABLE_NAME = "feature_descriptive_statistics";
  private static final String FEATURE_GROUP_DESCRIPTIVE_STATISTICS_TABLE_NAME = "feature_group_descriptive_statistics";
  private static final String FEATURE_GROUP_STATISTICS_TABLE_NAME = "feature_group_statistics";
  private static final String FEATURE_GROUP_COMMITS_TABLE_NAME = "feature_group_commit";
  private static final String TRAINING_DATASET_DESCRIPTIVE_STATISTICS_TABLE_NAME =
    "training_dataset_descriptive_statistics";
  private static final String TEST_DATASET_DESCRIPTIVE_STATISTICS_TABLE_NAME = "test_dataset_descriptive_statistics";
  private static final String VAL_DATASET_DESCRIPTIVE_STATISTICS_TABLE_NAME = "val_dataset_descriptive_statistics";
  
  private static final String SPLIT_NAME_TRAIN = "train";
  private static final String SPLIT_NAME_TEST = "test";
  private static final String SPLIT_NAME_VALIDATION = "validation";
  
  private static final String FOR_MIGRATION_FLAG = "for-migration";
  private static final String TO_BE_DELETED_FLAG = "to-be-deleted";
  
  private final static String GET_FEATURE_DESCRIPTIVE_STATISTICS = String.format(
    "SELECT id, feature_type, count, num_non_null_values, num_null_values, extended_statistics_path FROM %s WHERE " +
      "feature_name = '%s'", FEATURE_DESCRIPTIVE_STATISTICS_TABLE_NAME, FOR_MIGRATION_FLAG);
  
  private final static String GET_ORPHAN_STATISTICS =
    String.format("SELECT id, feature_type, count, extended_statistics_path FROM %s WHERE feature_name = '%s'",
      FEATURE_DESCRIPTIVE_STATISTICS_TABLE_NAME, TO_BE_DELETED_FLAG);
  
  private final static String GET_EARLIEST_FG_COMMITS_PER_FEATURE_GROUP =
    String.format("SELECT feature_group_id, MIN(commit_id) from %s GROUP BY feature_group_id",
      FEATURE_GROUP_COMMITS_TABLE_NAME);
  
  private final static String INSERT_FEATURE_DESCRIPTIVE_STATISTICS = String.format(
    "INSERT INTO %s (feature_name, feature_type, count, completeness, num_non_null_values, num_null_values, " +
      "approx_num_distinct_values, min, max, sum, mean, stddev, percentiles, distinctness, entropy, uniqueness, " +
      "exact_num_distinct_values, extended_statistics_path) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
      "?, ?)", FEATURE_DESCRIPTIVE_STATISTICS_TABLE_NAME);
  
  private final static String INSERT_FEATURE_GROUP_DESCRIPTIVE_STATISTICS =
    String.format("INSERT INTO %s (feature_group_statistics_id, feature_descriptive_statistics_id) VALUES (?, ?)",
      FEATURE_GROUP_DESCRIPTIVE_STATISTICS_TABLE_NAME);
  
  private final static String INSERT_TRAINING_DATASET_DESCRIPTIVE_STATISTICS =
    String.format("INSERT INTO %s (training_dataset_statistics_id, feature_descriptive_statistics_id) VALUES (?, ?)",
      TRAINING_DATASET_DESCRIPTIVE_STATISTICS_TABLE_NAME);
  
  private final static String INSERT_TEST_DATASET_DESCRIPTIVE_STATISTICS =
    String.format("INSERT INTO %s (training_dataset_statistics_id, feature_descriptive_statistics_id) VALUES (?, ?)",
      TEST_DATASET_DESCRIPTIVE_STATISTICS_TABLE_NAME);
  
  private final static String INSERT_VAL_DATASET_DESCRIPTIVE_STATISTICS =
    String.format("INSERT INTO %s (training_dataset_statistics_id, feature_descriptive_statistics_id) VALUES (?, ?)",
      VAL_DATASET_DESCRIPTIVE_STATISTICS_TABLE_NAME);
  
  private final static String UPDATE_FEATURE_GROUP_DESCRIPTIVE_STATISTICS =
    String.format("UPDATE %s SET window_start_commit_id = ? WHERE id = ?", FEATURE_GROUP_STATISTICS_TABLE_NAME);
  
  private final static String DELETE_FEATURE_DESCRIPTIVE_STATISTICS =
    String.format("DELETE FROM %s WHERE id = ?", FEATURE_DESCRIPTIVE_STATISTICS_TABLE_NAME);
  
  private final static String FEATURE_GROUP = "FEATURE_GROUP";
  private final static String TRAINING_DATASET = "TRAINING_DATASET";
  
  public StatisticsMigration() {
  }
  
  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("Starting migration of " + super.getClass().getName());
    
    try {
      setup();
      runMigration();
      close();
    } catch (ConfigurationException | SQLException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      close();
      throw new MigrationException(errorMsg, ex);
    } catch (IOException | IllegalAccessException | InstantiationException ex) {
      String errorMsg = "Could not migrate statistics";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    }
    LOGGER.info("Finished migration of " + super.getClass().getName());
  }
  
  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("Starting rollback of " + super.getClass().getName());
    try {
      setup();
      runRollback();
      close();
    } catch (ConfigurationException | SQLException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      close();
      throw new RollbackException(errorMsg, ex);
    }
    LOGGER.info("Finished rollback of " + super.toString());
  }
  
  public void runMigration()
    throws MigrationException, SQLException, IOException, IllegalAccessException, InstantiationException {
    PreparedStatement fdsStmt = null;
    PreparedStatement orphanStatsStmt = null;
    
    try {
      connection.setAutoCommit(false);
      
      // migrate feature descriptive statistics:
      // - insert statistics (per feature) into feature descriptive statistics table
      // - insert intermediate fg/td descriptive statistics rows
      // - update/remove statistics file. Only histograms, correlations, kll and unique values are kept in the file
      // - update start_commit_window in affected feature group statistic rows
      fdsStmt = connection.prepareStatement(GET_FEATURE_DESCRIPTIVE_STATISTICS);
      ResultSet fdsResultSet = fdsStmt.executeQuery();
      migrateFeatureDescriptiveStatistics(fdsResultSet);
      fdsStmt.close();
      
      // delete orphan statistics files. There are two possible reasons why stats file are orphan during the migration:
      // - if multiple FG statistics on the same commit id, only the most recent is migrated, the rest become orphan.
      // - if multiple TD statistics on the same dataset, only the most recent is migrated, the rest become orphan.
      orphanStatsStmt = connection.prepareStatement(GET_ORPHAN_STATISTICS);
      ResultSet orphanStatsResultSet = orphanStatsStmt.executeQuery();
      deleteOrphanStatisticsFiles(orphanStatsResultSet);
      orphanStatsStmt.close();
      
      connection.commit();
      connection.setAutoCommit(true);
    } finally {
      if (fdsStmt != null) {
        fdsStmt.close();
      }
      if (orphanStatsStmt != null) {
        orphanStatsStmt.close();
      }
    }
  }
  
  private void migrateFeatureDescriptiveStatistics(ResultSet fdsResultSet)
    throws SQLException, MigrationException, IOException, IllegalAccessException, InstantiationException {
    List<Integer> fdsIds = new ArrayList<>(); // keep track of temporary fds to be removed
    HashMap<Integer, Long> fgsEarliestFgCommitIds; // <fg id, earliest fg commit id>
    boolean updateFgStatistics = false; // whether to update feature group statistics
    
    // connections
    PreparedStatement insertFdsStmt = null;
    PreparedStatement insertFgFdsStmt = null;
    PreparedStatement updateFgsStmt = null;
    PreparedStatement insertTrainDatasetFdsStmt = null;
    PreparedStatement insertTestDatasetFdsStmt = null;
    PreparedStatement insertValDatasetFdsStmt = null;
    
    try {
      // earliest fg commit ids
      fgsEarliestFgCommitIds = getEarliestFgCommitIds();
      
      // fds connection
      insertFdsStmt = connection.prepareStatement(INSERT_FEATURE_DESCRIPTIVE_STATISTICS, new String[]{"id"});
      // fg stats connection
      insertFgFdsStmt = connection.prepareStatement(INSERT_FEATURE_GROUP_DESCRIPTIVE_STATISTICS); // intermediate
      // table between fg stats and fds
      updateFgsStmt = connection.prepareStatement(UPDATE_FEATURE_GROUP_DESCRIPTIVE_STATISTICS);
      // td stats connections
      insertTrainDatasetFdsStmt = connection.prepareStatement(INSERT_TRAINING_DATASET_DESCRIPTIVE_STATISTICS);
      insertTestDatasetFdsStmt = connection.prepareStatement(INSERT_TEST_DATASET_DESCRIPTIVE_STATISTICS);
      insertValDatasetFdsStmt = connection.prepareStatement(INSERT_VAL_DATASET_DESCRIPTIVE_STATISTICS);
      
      // per fds - migrate stats
      while (fdsResultSet.next()) {
        // extract fds column values
        int statisticsId = fdsResultSet.getInt(1); // this ID is the same for fg/td statistics and temporary fd stats
        String entityType = fdsResultSet.getString(2); // entity type is temp. stored in feature_type column
        int entityId = fdsResultSet.getInt(3); // feature group id, used to look for earliest commit id
        long commitTime = fdsResultSet.getLong(4); // commit time is temp. stored in count column
        long windowEndCommitId = fdsResultSet.getLong(5); // window end commit id
        String filePath = fdsResultSet.getString(6); // extended_stats_path contains the old stats file path
        
        LOGGER.info(
          String.format("[migrateFeatureDescriptiveStatistics] FdsResult: %s, %s, %s, %s, %s, %s", statisticsId,
            entityType, entityId, commitTime, windowEndCommitId, filePath));
        
        fdsIds.add(statisticsId); // track temporary fds ids, to be removed after migration
        
        if (entityType.equals(FEATURE_GROUP)) {
          // get window start commit id
          Long windowStartCommitId = fgsEarliestFgCommitIds.getOrDefault(entityId, null);
          LOGGER.info(String.format(
            "[migrateFeatureDescriptiveStatistics] -- window start commit is %s for feature group with id %s",
            windowStartCommitId == null ? "null" : String.valueOf(windowStartCommitId), entityId));
          if (windowStartCommitId == null && windowEndCommitId == 0) {
            windowEndCommitId = commitTime; // for non-time-travel-enabled feature groups, set end window as commit time
          }
          // migrate fg stats
          migrateFeatureGroupStatistics(statisticsId, filePath, windowStartCommitId, windowEndCommitId, insertFdsStmt,
            insertFgFdsStmt);
          // set window start commit id if time travel-enabled fg
          if (updateFeatureGroupStatisticsCommitWindow(updateFgsStmt, statisticsId, windowStartCommitId)) {
            updateFgStatistics = true;
          }
        } else if (entityType.equals(TRAINING_DATASET)) {
          migrateTrainingDatasetStatistics(statisticsId, filePath, commitTime, insertFdsStmt, insertTrainDatasetFdsStmt,
            insertTestDatasetFdsStmt, insertValDatasetFdsStmt);
        } else {
          throw new MigrationException(
            "Unknown entity type: " + entityType + ". Expected values are " + FEATURE_GROUP + " or " +
              TRAINING_DATASET);
        }
      }
      
      // update feature group statistics window start commits
      if (updateFgStatistics) {
        if (dryRun) {
          LOGGER.info(String.format("[migrateFeatureDescriptiveStatistics] Update FGS: %s", updateFgsStmt.toString()));
        } else {
          LOGGER.info(String.format("[migrateFeatureDescriptiveStatistics] Update FGS: %s", updateFgsStmt.toString()));
          updateFgsStmt.executeBatch();
        }
      }
      
      // delete temporary feature descriptive statistics
      deleteFeatureDescriptiveStatistics(fdsIds);
    } finally {
      if (insertFdsStmt != null) {
        insertFdsStmt.close();
      }
      if (insertFgFdsStmt != null) {
        insertFgFdsStmt.close();
      }
      if (updateFgsStmt != null) {
        updateFgsStmt.close();
      }
      if (insertTrainDatasetFdsStmt != null) {
        insertTrainDatasetFdsStmt.close();
      }
      if (insertTestDatasetFdsStmt != null) {
        insertTestDatasetFdsStmt.close();
      }
      if (insertValDatasetFdsStmt != null) {
        insertValDatasetFdsStmt.close();
      }
    }
  }
  
  private void deleteOrphanStatisticsFiles(ResultSet orphanStatsResultSet) throws SQLException, IOException {
    List<Integer> fdsIds = new ArrayList<>(); // keep track of fds ids to be removed
    
    // per orphan statistics - delete hdfs file
    while (orphanStatsResultSet.next()) {
      int statisticsId =
        orphanStatsResultSet.getInt(1); // this ID is the same for fg/td statistics and temporary fd stats
      String entityType = orphanStatsResultSet.getString(2); // entity type is temp. stored in feature_type column
      int entityId = orphanStatsResultSet.getInt(3); // entity id. If FG entity, used to look for earliest commit id
      String filePath = orphanStatsResultSet.getString(4); // extended_stats_path contains the old stats file path
      
      fdsIds.add(statisticsId);  // track temporary fds ids, to be removed
      
      if (dryRun) {
        LOGGER.info(
          String.format("[deleteOrphanStatisticsFiles] Deleting orphan stats file: %s, %s, %s, %s", statisticsId,
            entityType, entityId, filePath));
      } else {
        LOGGER.info(
          String.format("[deleteOrphanStatisticsFiles] Deleting orphan stats file: %s, %s, %s, %s", statisticsId,
            entityType, entityId, filePath));
        dfso.rm(filePath, true);
      }
    }
    
    // delete temporary feature descriptive statistics
    deleteFeatureDescriptiveStatistics(fdsIds);
  }
  
  private void migrateFeatureGroupStatistics(int statisticsId, String filePath, Long windowStartCommitId,
    Long windowEndCommitId, PreparedStatement insertFdsStmt, PreparedStatement insertIntermediateStmt)
    throws SQLException, IOException, MigrationException, IllegalAccessException, InstantiationException {
    // read and parse old hdfs file with statistics
    Collection<ExpatFeatureDescriptiveStatistics> fdsList = readAndParseLegacyStatistics(filePath);
    
    // get stats file parent directory
    Path oldFilePath = new Path(filePath);
    Path parentDirPath = oldFilePath.getParent();
    
    // get owner, permissions and group
    FileStatus fileStatus = dfso.getFileStatus(oldFilePath);
    
    // insert feature descriptive statistics
    insertFeatureDescriptiveStatistics(statisticsId, fdsList, insertIntermediateStmt, insertFdsStmt,
      windowStartCommitId, windowEndCommitId, false, null, parentDirPath, fileStatus);
    
    // remove old hfds file
    if (dryRun) {
      LOGGER.info(String.format("[migrateFeatureGroupStatistics] Remove old hdfs stats file at: %s", filePath));
    } else {
      LOGGER.info(String.format("[migrateFeatureGroupStatistics] Remove old hdfs stats file at: %s", filePath));
      dfso.rm(filePath, false);
    }
  }
  
  private void migrateTrainingDatasetStatistics(int statisticsId, String filePath, Long commitTime,
    PreparedStatement insertFdsStmt, PreparedStatement insertTrainDatasetFdsStmt,
    PreparedStatement insertTestDatasetFdsStmt, PreparedStatement insertValDatasetFdsStmt)
    throws SQLException, IOException, MigrationException, IllegalAccessException, InstantiationException {
    
    if (!dfso.exists(filePath)) {
      LOGGER.info("[migrateTrainingDatasetStatistics] file path does not exist: " + filePath);
      return;
    }
    
    // get owner, permissions and group
    Path oldFilePath = new Path(filePath);
    FileStatus fileStatus = dfso.getFileStatus(oldFilePath);
    
    if (dfso.isDir(filePath)) { // training dataset with splits
      Path parentDirPath = oldFilePath;
      
      // train split stats
      String trainSplitFilePath = filePath + "/" + SPLIT_NAME_TRAIN + "_" + commitTime + ".json";
      Collection<ExpatFeatureDescriptiveStatistics> fdsList = readAndParseLegacyStatistics(trainSplitFilePath);
      insertFeatureDescriptiveStatistics(statisticsId, fdsList, insertTrainDatasetFdsStmt, insertFdsStmt, null,
        commitTime, false, SPLIT_NAME_TRAIN, parentDirPath, fileStatus);
      if (dryRun) {
        LOGGER.info(
          String.format("[migrateTrainingDatasetStatistics] Remove old hdfs stats file at: %s", trainSplitFilePath));
      } else {
        LOGGER.info(
          String.format("[migrateTrainingDatasetStatistics] Remove old hdfs stats file at: %s", trainSplitFilePath));
        dfso.rm(trainSplitFilePath, false); // remove old hfds file
      }
      
      // test split stats
      String testSplitFilePath = filePath + "/" + SPLIT_NAME_TEST + "_" + commitTime + ".json";
      fdsList = readAndParseLegacyStatistics(testSplitFilePath);
      insertFeatureDescriptiveStatistics(statisticsId, fdsList, insertTestDatasetFdsStmt, insertFdsStmt, null,
        commitTime, false, SPLIT_NAME_TEST, parentDirPath, fileStatus);
      if (dryRun) {
        LOGGER.info(
          String.format("[migrateTrainingDatasetStatistics] Remove old hdfs stats file at: %s", testSplitFilePath));
      } else {
        LOGGER.info(
          String.format("[migrateTrainingDatasetStatistics] Remove old hdfs stats file at: %s", testSplitFilePath));
        dfso.rm(testSplitFilePath, false); // remove old hfds file
      }
      
      // val split stats
      String valSplitFilePath = filePath + "/" + SPLIT_NAME_VALIDATION + "_" + commitTime + ".json";
      if (dfso.exists(valSplitFilePath)) {
        fdsList = readAndParseLegacyStatistics(valSplitFilePath);
        insertFeatureDescriptiveStatistics(statisticsId, fdsList, insertValDatasetFdsStmt, insertFdsStmt, null,
          commitTime, false, SPLIT_NAME_VALIDATION, parentDirPath, fileStatus);
        if (dryRun) {
          LOGGER.info(
            String.format("[migrateTrainingDatasetStatistics] Remove old hdfs stats file at: %s", valSplitFilePath));
        } else {
          LOGGER.info(
            String.format("[migrateTrainingDatasetStatistics] Remove old hdfs stats file at: %s", valSplitFilePath));
          dfso.rm(valSplitFilePath, false); // remove old hfds file
        }
      }
    } else { // otherwise, either whole training dataset statistics or tr. functions statistics json file
      Collection<ExpatFeatureDescriptiveStatistics> fdsList = readAndParseLegacyStatistics(filePath);
      boolean forTransformation = filePath.contains("transformation_fn");
      Path parentDirPath = oldFilePath.getParent();
      insertFeatureDescriptiveStatistics(statisticsId, fdsList, insertTrainDatasetFdsStmt, insertFdsStmt, null,
        commitTime, forTransformation, null, parentDirPath, fileStatus);
      if (dryRun) {
        LOGGER.info(String.format("[migrateTrainingDatasetStatistics] Remove old hdfs stats file at: %s", filePath));
      } else {
        LOGGER.info(String.format("[migrateTrainingDatasetStatistics] Remove old hdfs stats file at: %s", filePath));
        dfso.rm(filePath, false); // remove old hfds file
      }
    }
  }
  
  private HashMap<Integer, Long> getEarliestFgCommitIds() throws SQLException {
    PreparedStatement getEarliestFgCommitStmt = null;
    try {
      getEarliestFgCommitStmt = connection.prepareStatement(GET_EARLIEST_FG_COMMITS_PER_FEATURE_GROUP);
      ResultSet earliestFgCommits = getEarliestFgCommitStmt.executeQuery();
      
      HashMap<Integer, Long> fgsEarliestFgCommitIds = new HashMap<>();
      while (earliestFgCommits.next()) {
        fgsEarliestFgCommitIds.put(earliestFgCommits.getInt(1), earliestFgCommits.getLong(2));
      }
      
      LOGGER.info(
        String.format("[getEarliestFgCommitIds] list of earliest commits. Feature Groups: [%s], Commit IDs: [%s]",
          fgsEarliestFgCommitIds.keySet().stream().map(String::valueOf).collect(Collectors.joining(",")),
          fgsEarliestFgCommitIds.values().stream().map(String::valueOf).collect(Collectors.joining(","))));
      
      return fgsEarliestFgCommitIds;
    } finally {
      if (getEarliestFgCommitStmt != null) {
        getEarliestFgCommitStmt.close();
      }
    }
  }
  
  private Collection<ExpatFeatureDescriptiveStatistics> readAndParseLegacyStatistics(String filePath)
    throws IOException {
    // read file content
    String fileContent = dfso.cat(filePath);
    // parse feature descriptive statistics
    return ExpatFeatureDescriptiveStatistics.parseStatisticsJsonString(fileContent);
  }
  
  private void insertFeatureDescriptiveStatistics(int statisticsId,
    Collection<ExpatFeatureDescriptiveStatistics> fdsList, PreparedStatement insertIntermediateStmt,
    PreparedStatement insertFdsStmt, Long windowStartCommitId, Long windowEndCommitId, boolean forTransformation,
    String splitName, Path dirPath, FileStatus fileStatus)
    throws SQLException, MigrationException, IOException, IllegalAccessException, InstantiationException {
    if (fdsList.isEmpty()) {
      return; // nothing to insert
    }
    
    // insert feature descriptive statistics rows
    for (ExpatFeatureDescriptiveStatistics fds : fdsList) {
      // create extended statistics file, if needed
      fds.extendedStatistics =
        createExtendedStatisticsFile(windowStartCommitId, windowEndCommitId, fds.featureName, fds.extendedStatistics,
          forTransformation, splitName, dirPath, fileStatus);
      // set statement parameters
      setFdsStatementParameters(insertFdsStmt, fds);
      // add to batch
      insertFdsStmt.addBatch();
    }
    
    if (dryRun) {
      LOGGER.info(
        String.format("[insertFeatureDescriptiveStatistics] Insert batch of FDS: %s", insertFdsStmt.toString()));
    } else {
      LOGGER.info(
        String.format("[insertFeatureDescriptiveStatistics] Insert batch of FDS: %s", insertFdsStmt.toString()));
      
      // insert fds
      insertFdsStmt.executeBatch();
      
      // insert intermediate table rows
      ResultSet generatedKeys = insertFdsStmt.getGeneratedKeys();
      while (generatedKeys.next()) {
        Integer fdsId = generatedKeys.getInt(1);
        insertIntermediateStmt.setInt(1, statisticsId);
        insertIntermediateStmt.setInt(2, fdsId);
        insertIntermediateStmt.addBatch();
      }
      insertIntermediateStmt.executeBatch();
    }
  }
  
  private void setFdsStatementParameters(PreparedStatement insertFdsStmt, ExpatFeatureDescriptiveStatistics fds)
    throws SQLException, MigrationException {
    setFdsPreparedStatementParameter(insertFdsStmt, 1, Types.VARCHAR, fds.featureName);
    setFdsPreparedStatementParameter(insertFdsStmt, 2, Types.VARCHAR, fds.featureType);
    setFdsPreparedStatementParameter(insertFdsStmt, 3, Types.BIGINT, fds.count);
    setFdsPreparedStatementParameter(insertFdsStmt, 4, Types.FLOAT, fds.completeness);
    setFdsPreparedStatementParameter(insertFdsStmt, 5, Types.BIGINT, fds.numNonNullValues);
    setFdsPreparedStatementParameter(insertFdsStmt, 6, Types.BIGINT, fds.numNullValues);
    setFdsPreparedStatementParameter(insertFdsStmt, 7, Types.BIGINT, fds.approxNumDistinctValues);
    setFdsPreparedStatementParameter(insertFdsStmt, 8, Types.FLOAT, fds.min);
    setFdsPreparedStatementParameter(insertFdsStmt, 9, Types.FLOAT, fds.max);
    setFdsPreparedStatementParameter(insertFdsStmt, 10, Types.FLOAT, fds.sum);
    setFdsPreparedStatementParameter(insertFdsStmt, 11, Types.FLOAT, fds.mean);
    setFdsPreparedStatementParameter(insertFdsStmt, 12, Types.FLOAT, fds.stddev);
    setFdsPreparedStatementParameter(insertFdsStmt, 13, Types.BLOB, convertPercentilesToBlob(fds.percentiles));
    setFdsPreparedStatementParameter(insertFdsStmt, 14, Types.FLOAT, fds.distinctness);
    setFdsPreparedStatementParameter(insertFdsStmt, 15, Types.FLOAT, fds.entropy);
    setFdsPreparedStatementParameter(insertFdsStmt, 16, Types.FLOAT, fds.uniqueness);
    setFdsPreparedStatementParameter(insertFdsStmt, 17, Types.BIGINT, fds.exactNumDistinctValues);
    setFdsPreparedStatementParameter(insertFdsStmt, 18, Types.VARCHAR, fds.extendedStatistics);
  }
  
  private void setFdsPreparedStatementParameter(PreparedStatement stmt, int paramPosition, int sqlType, Object value)
    throws SQLException, MigrationException {
    if (value == null) {
      stmt.setNull(paramPosition, sqlType);
      return;
    }
    switch (sqlType) {
      case Types.BIGINT:
        stmt.setLong(paramPosition, (Long) value);
        break;
      case Types.VARCHAR:
        stmt.setString(paramPosition, (String) value);
        break;
      case Types.BLOB:
        stmt.setBlob(paramPosition, (Blob) value);
        break;
      case Types.FLOAT:
        stmt.setDouble(paramPosition, (Double) value);
        break;
      case Types.INTEGER:
        stmt.setInt(paramPosition, (Integer) value);
        break;
      default:
        throw new MigrationException("Unknown sql type '" + String.valueOf(sqlType) + "' for feature descriptive " +
          "statistics parameter in position: " + String.valueOf(paramPosition));
    }
  }
  
  private void deleteFeatureDescriptiveStatistics(Collection<Integer> fdsIds) throws SQLException {
    if (fdsIds.isEmpty()) {
      return; // nothing to be deleted
    }
    PreparedStatement deleteFdsStmt = null;
    try {
      deleteFdsStmt = connection.prepareStatement(DELETE_FEATURE_DESCRIPTIVE_STATISTICS);
      for (Integer fdsId : fdsIds) {
        deleteFdsStmt.setInt(1, fdsId);
        deleteFdsStmt.addBatch();
      }
      if (dryRun) {
        LOGGER.info(
          String.format("[deleteFeatureDescriptiveStatistics] Delete batch of FDS: %s", deleteFdsStmt.toString()));
      } else {
        LOGGER.info(
          String.format("[deleteFeatureDescriptiveStatistics] Delete batch of FDS: %s", deleteFdsStmt.toString()));
        deleteFdsStmt.executeBatch();
      }
    } finally {
      if (deleteFdsStmt != null) {
        deleteFdsStmt.close();
      }
    }
  }
  
  private Blob convertPercentilesToBlob(List<Double> percentiles) throws SQLException {
    return percentiles != null && !percentiles.isEmpty() ? new SerialBlob(convertPercentilesToByteArray(percentiles)) :
      null;
  }
  
  private String createExtendedStatisticsFile(Long windowStartCommitId, Long windowEndCommitId, String featureName,
    String extendedStatistics, Boolean forTransformation, String splitName, Path dirPath, FileStatus fileStatus)
    throws IOException, MigrationException, SQLException, IllegalAccessException, InstantiationException {
    if (extendedStatistics == null || extendedStatistics.isEmpty()) {
      return null; // no extended stats to persist
    }
    // Persist extended statistics on a file in hopsfs
    Path filePath;
    if (forTransformation) {
      filePath =
        new Path(dirPath, transformationFnStatisticsFileName(windowStartCommitId, windowEndCommitId, featureName));
    } else {
      if (splitName != null) {
        filePath = new Path(dirPath, splitStatisticsFileName(splitName, windowEndCommitId, featureName));
      } else {
        filePath = new Path(dirPath, statisticsFileName(windowStartCommitId, windowEndCommitId, featureName));
      }
    }
    //    extendedStatistics = sanitizeExtendedStatistics(extendedStatistics);
    // get owner, permissions and group
    String owner = fileStatus.getOwner();
    FsPermission permissions = fileStatus.getPermission();
    String group = fileStatus.getGroup();
    if (dryRun) {
      LOGGER.info(String.format(
        "[createExtendedStatisticsFile] Create FDS hdfs file at: %s with owner: %s, group: %s and content: %s",
        filePath, owner, group, "extendedStatistics"));
    } else {
      LOGGER.info(String.format(
        "[createExtendedStatisticsFile] Create FDS hdfs file at: %s with owner: %s, group: %s and content: %s",
        filePath, owner, group, "extendedStatistics"));
      // create file
      dfso.create(filePath, extendedStatistics);
      setOwnershipAndPermissions(filePath, owner, permissions, group, dfso);
    }
    return filePath.toString();
  }
  
  private String transformationFnStatisticsFileName(Long startCommitTime, Long endCommitTime, String featureName) {
    String name = "transformation_fn_";
    name += startCommitTime != null ? startCommitTime + "_" : "";
    return name + endCommitTime + "_" + featureName + ".json";
  }
  
  private String splitStatisticsFileName(String splitName, Long commitTime, String featureName) {
    return commitTime + "_" + splitName + "_" + featureName + ".json";
  }
  
  private String statisticsFileName(Long startCommitTime, Long endCommitTime, String featureName) {
    String name = startCommitTime != null ? startCommitTime + "_" : "";
    return name + endCommitTime + "_" + featureName + ".json";
  }
  
  private boolean updateFeatureGroupStatisticsCommitWindow(PreparedStatement updateFgsStmt, Integer fgStatisticsId,
    Long windowStartCommitTime) throws SQLException {
    if (windowStartCommitTime == null) {
      return false; // skip update, no time travel-enabled feature group
    }
    updateFgsStmt.setLong(1, windowStartCommitTime);
    updateFgsStmt.setInt(2, fgStatisticsId);
    updateFgsStmt.addBatch();
    return true;
  }
  
  private byte[] convertPercentilesToByteArray(List<Double> percentilesList) {
    if (percentilesList == null) {
      return null;
    }
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(percentilesList);
      oos.flush();
      return bos.toByteArray();
    } catch (IOException e) {
      LOGGER.info("Cannot convert percentiles map to byte array");
    }
    return null;
  }
  
  private void setOwnershipAndPermissions(Path filePath, String username, FsPermission permissions, String group,
    DistributedFileSystemOps dfso) throws IOException {
    dfso.setOwner(filePath, username, group);
    if (permissions != null) {
      dfso.setPermission(filePath, permissions);
    }
  }
  
  public void runRollback() throws RollbackException {
  
  }
  
  protected void setup() throws ConfigurationException, SQLException {
    connection = DbConnectionFactory.getConnection();
    
    Configuration conf = ConfigurationBuilder.getConfiguration();
    hopsUser = conf.getString(ExpatConf.HOPS_CLIENT_USER);
    if (hopsUser == null) {
      throw new ConfigurationException(ExpatConf.HOPS_CLIENT_USER + " cannot be null");
    }
    dfso = HopsClient.getDFSO(hopsUser);
    dryRun = conf.getBoolean(ExpatConf.DRY_RUN);
    inodeController = new ExpatInodeController(this.connection);
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
}
