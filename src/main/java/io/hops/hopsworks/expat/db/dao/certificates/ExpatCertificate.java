/*
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
package io.hops.hopsworks.expat.db.dao.certificates;

public class ExpatCertificate {
  private final String projectName;
  private final String username;
  private String cipherPassword;
  private String plainPassword;
  private byte[] keyStore;
  private byte[] trustStore;
  
  public ExpatCertificate(String projectName, String username) {
    this.projectName = projectName;
    this.username = username;
  }

  public ExpatCertificate(String projectName, String username, String cipherPassword) {
    this.projectName = projectName;
    this.username = username;
    this.cipherPassword = cipherPassword;
  }
  
  public String getProjectName() {
    return projectName;
  }
  
  public String getUsername() {
    return username;
  }
  
  public String getCipherPassword() {
    return cipherPassword;
  }
  
  public void setCipherPassword(String cipherPassword) {
    this.cipherPassword = cipherPassword;
  }
  
  public String getPlainPassword() {
    return plainPassword;
  }
  
  public void setPlainPassword(String plainPassword) {
    this.plainPassword = plainPassword;
  }
  
  public byte[] getKeyStore() {
    return keyStore;
  }
  
  public void setKeyStore(byte[] keyStore) {
    this.keyStore = keyStore;
  }
  
  public byte[] getTrustStore() {
    return trustStore;
  }
  
  public void setTrustStore(byte[] trustStore) {
    this.trustStore = trustStore;
  }
  
  @Override
  public String toString() {
    return projectName + "__" + username;
  }
}
