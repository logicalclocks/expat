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
