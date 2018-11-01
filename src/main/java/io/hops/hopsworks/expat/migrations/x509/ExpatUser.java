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

public class ExpatUser {
  private final int uid;
  private final String username;
  private final String password;
  private final String email;
  private final String orcid;
  private final String organization;
  private final String country;
  private final String city;
  
  public ExpatUser(int uid, String username, String password, String email, String orcid, String organization,
      String country, String city) {
    this.uid = uid;
    this.username = username;
    this.password = password;
    this.email = email;
    this.orcid = orcid;
    this.organization = organization;
    this.country = country;
    this.city = city;
  }
  
  public int getUid() {
    return uid;
  }
  
  public String getUsername() {
    return username;
  }
  
  public String getPassword() {
    return password;
  }
  
  public String getEmail() {
    return email;
  }
  
  public String getOrcid() {
    return orcid;
  }
  
  public String getOrganization() {
    return organization;
  }
  
  public String getCountry() {
    return country;
  }
  
  public String getCity() {
    return city;
  }
}
