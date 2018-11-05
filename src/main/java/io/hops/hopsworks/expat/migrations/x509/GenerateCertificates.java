/**
 * This file is part of Expat
 * Copyright (C) 2018, Logical Clocks AB. All rights reserved
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
package io.hops.hopsworks.expat.migrations.x509;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.Utils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class GenerateCertificates {
  private static final Logger LOGGER = LogManager.getLogger(GenerateCertificates.class);
  
  private final static String GET_USER_BY_USERNAME = "SELECT * FROM users WHERE username = ?";
  private final static String GET_USER_BY_EMAIL = "SELECT * FROM users WHERE email = ?";
  private final static String GET_ADDRESS_BY_UID = "SELECT * FROM address WHERE uid = ?";
  private final static String GET_ORGANIZATION_BY_UID = "SELECT * FROM organization WHERE uid = ?";
  
  protected Path certsBackupDir;
  protected Configuration config;
  protected Path masterPwdPath;
  protected String intermediateCA;
  protected String masterPassword;
  protected String userCertsScript;
  protected Connection connection;
  
  protected void setup(String backupDirPrefix)
      throws MigrationException, ConfigurationException, IOException, SQLException {
    String userHome = System.getProperty("user.home");
    if (userHome == null) {
      throw new MigrationException("Could not get user home");
    }
    LocalDateTime now = LocalDateTime.now();
    certsBackupDir = Paths.get(userHome, backupDirPrefix + "_certs_backup_" + now.toString());
  
    try {
      FileUtils.forceMkdir(certsBackupDir.toFile());
    } catch (IOException ex) {
      throw new MigrationException("Could not create certs backup directory", ex);
    }
    
    config = ConfigurationBuilder.getConfiguration();
    masterPwdPath = Paths.get(config.getString(ExpatConf.MASTER_PWD_FILE_KEY));
    intermediateCA = config.getString(ExpatConf.INTERMEDIATE_CA_PATH);
    masterPassword = Files.toString(masterPwdPath.toFile(), Charset.defaultCharset());
    userCertsScript = config.getString(ExpatConf.CREATE_USER_CERT_SCRIPT);
    connection = DbConnectionFactory.getConnection();
  }
  
  protected void generateNewCertsAndUpdateDb(Map<ExpatCertificate, ExpatUser> certificates, String print)
      throws SQLException, IOException {
    int total = certificates.size();
    LOGGER.info("Going to regenerate " + total + " certificates");
    LOGGER.info("Start generating new " + print + " Certificates");
    int idx = 1;
    for (Map.Entry<ExpatCertificate, ExpatUser> entry : certificates.entrySet()) {
      generateCertificate(entry.getKey(), entry.getValue(), idx, total);
      idx++;
    }
    LOGGER.info("Start updating certificates");
    updateCertificatesInDB(certificates.keySet(), connection);
  }
  
  private void generateCertificate(ExpatCertificate userCert, ExpatUser user, int idx, int total) throws IOException {
    // Move previous certificates for backup
    String id = userCert.getProjectName() + "__" + userCert.getUsername();
    LOGGER.info("Generating new certificate for " + userCert);
    
    String certId = id + ".cert.pem";
    File oldCert = Paths.get(intermediateCA, "certs", certId).toFile();
    File backupCert = Paths.get(certsBackupDir.toString(), certId).toFile();
    String keyId = id + ".key.pem";
    File oldKey = Paths.get(intermediateCA, "private", keyId).toFile();
    File backupKey = Paths.get(certsBackupDir.toString(), keyId).toFile();
    
    if (oldCert.exists()) {
      FileUtils.moveFile(oldCert, backupCert);
    }
    if (oldKey.exists()) {
      FileUtils.moveFile(oldKey, backupKey);
    }
    
    // Generate certificate
    List<String> commands = new ArrayList<>(9);
    commands.add("/usr/bin/sudo");
    commands.add(Paths.get(intermediateCA, userCertsScript).toString());
    commands.add(id);
    commands.add(user.getCountry());
    commands.add(user.getCity());
    commands.add(user.getOrganization());
    commands.add(user.getEmail());
    commands.add(user.getOrcid());
    commands.add(userCert.getPlainPassword());
    Utils.executeCommand(commands, false);
    
    File keyStoreFile = Paths.get("/tmp", id + "__kstore.jks").toFile();
    File trustStoreFile = Paths.get("/tmp", id + "__tstore.jks").toFile();
    byte[] keyStore, trustStore;
    try (FileInputStream fis = new FileInputStream(keyStoreFile)) {
      keyStore = ByteStreams.toByteArray(fis);
    }
    try (FileInputStream fis = new FileInputStream(trustStoreFile)) {
      trustStore = ByteStreams.toByteArray(fis);
    }
    userCert.setKeyStore(keyStore);
    userCert.setTrustStore(trustStore);
    LOGGER.info("Finished generating new certificate for " + userCert + " - " + idx + "/" + total);
  }
  
  protected ExpatUser getExpatUserByUsername(String username) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(GET_USER_BY_USERNAME);
    stmt.setString(1, username);
    return getExpatUser(stmt);
  }
  
  protected ExpatUser getExpatUserByEmail(String email) throws SQLException {
    PreparedStatement stmt = connection.prepareStatement(GET_USER_BY_EMAIL);
    stmt.setString(1, email);
    return getExpatUser(stmt);
  }
  
  private ExpatUser getExpatUser(PreparedStatement stmt) throws SQLException {
    PreparedStatement addressStmt = null, orgStmt = null;
    ResultSet userRS = null, addressRS = null, orgRS = null;
    
    try {
      userRS = stmt.executeQuery();
      userRS.next();
      Integer uid = userRS.getInt("uid");
      String email = userRS.getString("email");
      String orcid = userRS.getString("orcid");
      String userPassword = userRS.getString("password");
      String username = userRS.getString("username");
    
      addressStmt = connection.prepareStatement(GET_ADDRESS_BY_UID);
      addressStmt.setInt(1, uid);
      addressRS = addressStmt.executeQuery();
      addressRS.next();
      String country = addressRS.getString("country");
      String city = addressRS.getString("city");
    
    
      orgStmt = connection.prepareStatement(GET_ORGANIZATION_BY_UID);
      orgStmt.setInt(1, uid);
      orgRS = orgStmt.executeQuery();
      orgRS.next();
      String organization = orgRS.getString("org_name");
    
    
      return new ExpatUser(uid, username, userPassword, email, orcid, organization, country, city);
    } finally {
      if (userRS != null) {
        userRS.close();
      }
      if (addressRS != null) {
        addressRS.close();
      }
      if (orgRS != null) {
        orgRS.close();
      }
      if (stmt != null) {
        stmt.close();
      }
      if (addressStmt != null) {
        addressStmt.close();
      }
      if (orgStmt != null) {
        orgStmt.close();
      }
    }
  }
  
  abstract void updateCertificatesInDB(Set<ExpatCertificate> certificates, Connection connection)
      throws SQLException;
}
