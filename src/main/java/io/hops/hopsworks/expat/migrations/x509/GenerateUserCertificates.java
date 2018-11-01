/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.hopsworks.expat.migrations.x509;

import io.hops.hopsworks.common.util.HopsUtils;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;


public class GenerateUserCertificates extends GenerateCertificates implements MigrateStep {
  private final static Logger LOGGER = Logger.getLogger(GenerateUserCertificates.class.getName());
  private final static String SELECT_USER_CERTS = "SELECT * FROM user_certs";
  private final static String UPDATE_USER_CERTS = "UPDATE user_certs SET user_key = ?, user_cert = ? WHERE " +
      "projectname = ? && username = ?";
  
  @Override
  public void migrate() throws MigrationException {
    try {
      // Important!
      setup("UserCertificates");
      
      LOGGER.log(Level.INFO, "Getting all User Certificates");
      Map<ExpatCertificate, ExpatUser> userCerts = getUserCerts();
      
      generateNewCertsAndUpdateDb(userCerts, "User");
      
      LOGGER.log(Level.INFO, "Finished migration of User Certificates.");
      LOGGER.log(Level.INFO, ">>> You should revoke certificates and clean manually backup dir with previous certs: " +
          certsBackupDir.toString());
    } catch (ConfigurationException | SQLException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.log(Level.SEVERE, errorMsg);
      throw new MigrationException(errorMsg, ex);
    } catch (IOException ex) {
      String errorMsg = "Could not read master password";
      LOGGER.log(Level.SEVERE, errorMsg);
      throw new MigrationException(errorMsg, ex);
    } catch (Exception ex) {
      String errorMsg = "Could not decrypt user password";
      LOGGER.log(Level.SEVERE, errorMsg);
      throw new MigrationException(errorMsg, ex);
    }
  }
  
  @Override
  public void rollback() throws RollbackException {
    try {
      migrate();
    } catch (MigrationException ex) {
      throw new RollbackException("Could not rollback User Certificates", ex);
    }
  }
  
  
  
  private Map<ExpatCertificate, ExpatUser> getUserCerts() throws Exception {
    Map<ExpatCertificate, ExpatUser> userCerts = new HashMap<>();
    ResultSet certsRS = null;
    Statement userCertsStmt = connection.createStatement();
    try {
      certsRS = userCertsStmt.executeQuery(SELECT_USER_CERTS);
      
      while (certsRS.next()) {
        String projectName = certsRS.getString("projectname");
        String username = certsRS.getString("username");
        String password = certsRS.getString("user_key_pwd");
    
        ExpatCertificate cert = new ExpatCertificate(projectName, username);
        cert.setCipherPassword(password);
        ExpatUser user = getExpatUserByUsername(username);
        cert.setPlainPassword(HopsUtils.decrypt(user.getPassword(), password, masterPassword));
        userCerts.put(cert, user);
      }
      return userCerts;
    } finally {
      if (certsRS != null) {
        certsRS.close();
      }
      if (userCertsStmt != null) {
        userCertsStmt.close();
      }
    }
  }
  
  void updateCertificatesInDB(Set<ExpatCertificate> userCerts, Connection conn) throws SQLException {
    
    PreparedStatement updateStmt = null;
    try {
      conn.setAutoCommit(false);
      updateStmt = conn.prepareStatement(UPDATE_USER_CERTS);
      for (ExpatCertificate uc : userCerts) {
        updateStmt.setBytes(1, uc.getKeyStore());
        updateStmt.setBytes(2, uc.getTrustStore());
        updateStmt.setString(3, uc.getProjectName());
        updateStmt.setString(4, uc.getUsername());
        updateStmt.addBatch();
        LOGGER.log(Level.INFO, "Added " + uc + " to Tx batch");
      }
      updateStmt.executeBatch();
      conn.commit();
      LOGGER.log(Level.INFO, "Finished updating database");
    } finally {
      if (updateStmt != null) {
        updateStmt.close();
      }
      conn.setAutoCommit(true);
    }
  }
}
