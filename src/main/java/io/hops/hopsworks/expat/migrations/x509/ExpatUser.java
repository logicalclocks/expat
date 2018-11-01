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
