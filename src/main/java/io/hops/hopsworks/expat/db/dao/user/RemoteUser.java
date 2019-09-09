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

package io.hops.hopsworks.expat.db.dao.user;

public class RemoteUser {
  private int type;
  private String auth_key;
  private String uuid;
  private int uid;

  public RemoteUser(int type, String auth_key, String uuid, int uid) {
    this.type = type;
    this.auth_key = auth_key;
    this.uuid = uuid;
    this.uid = uid;
  }

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  public String getAuth_key() {
    return auth_key;
  }

  public void setAuth_key(String auth_key) {
    this.auth_key = auth_key;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public int getUid() {
    return uid;
  }

  public void setUid(int uid) {
    this.uid = uid;
  }
}
