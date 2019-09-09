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

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ExpatUserFacade {

  private static final Logger LOGGER = LogManager.getLogger(ExpatUserFacade.class);

  private final static String GET_USERS = "SELECT * FROM users";
  private final static String GET_LOCAL_USERS = "SELECT * FROM users WHERE mode = 0";
  private final static String GET_USER_BY_USERNAME = "SELECT * FROM users WHERE username = ?";
  private final static String GET_USER_BY_EMAIL = "SELECT * FROM users WHERE email = ?";
  private final static String GET_ADDRESS_BY_UID = "SELECT * FROM address WHERE uid = ?";
  private final static String GET_ORGANIZATION_BY_UID = "SELECT * FROM organization WHERE uid = ?";

  private final static String UPDATE_PWD = "UPDATE users SET password = ? WHERE uid = ?";
  private final static String UPDATE_MODE = "UPDATE users SET mode = ? WHERE uid = ?";

  public ExpatUser getExpatUserByUsername(Connection connection,
                                             String username) throws SQLException {
    return getSingleExpatUser(connection, GET_USER_BY_USERNAME, username);
  }

  public ExpatUser getExpatUserByEmail(Connection connection,
                                          String email) throws SQLException {
    return getSingleExpatUser(connection, GET_USER_BY_EMAIL, email);
  }

  private ExpatUser getSingleExpatUser(Connection connection,
                                       String query, String param) throws SQLException{
    ResultSet userRS = null;
    try (PreparedStatement stmt = connection.prepareStatement(query)) {
      stmt.setString(1, param);
      userRS = stmt.executeQuery();

      if (userRS.next()) {
        return getExpatUser(connection, userRS);
      } else {
        return null;
      }

    } finally {
      if (userRS != null) {
        userRS.close();
      }
    }
  }

  public List<ExpatUser> getExpatUsers(Connection connection) throws SQLException {
    List<ExpatUser> result = new ArrayList<>();
    ResultSet userRS = null;

    try (PreparedStatement stmt = connection.prepareStatement(GET_USERS)) {
      userRS = stmt.executeQuery();
      while (userRS.next()) {
        result.add(getExpatUser(connection, userRS));
      }
    } finally {
      if (userRS != null) {
        userRS.close();
      }
    }

    return result;
  }

  public List<ExpatUser> getLocalUsers(Connection connection) throws SQLException {
    List<ExpatUser> result = new ArrayList<>();
    ResultSet userRS = null;

    try (PreparedStatement stmt = connection.prepareStatement(GET_LOCAL_USERS)) {
      userRS = stmt.executeQuery();
      while (userRS.next()) {
        result.add(getExpatUser(connection, userRS));
      }
    } finally {
      if (userRS != null) {
        userRS.close();
      }
    }

    return result;
  }

  private ExpatUser getExpatUser(Connection connection,
                                 ResultSet userRS) throws SQLException {
    PreparedStatement addressStmt = null, orgStmt = null;
    ResultSet addressRS = null, orgRS = null;

    try {
      int uid = userRS.getInt("uid");
      String email = userRS.getString("email");
      String orcid = userRS.getString("orcid");
      String userPassword = userRS.getString("password");
      String username = userRS.getString("username");
      String salt = userRS.getString("salt");

      addressStmt = connection.prepareStatement(GET_ADDRESS_BY_UID);
      addressStmt.setInt(1, uid);
      addressRS = addressStmt.executeQuery();
      String country = "";
      String city = "";
      if (addressRS.next()) {
        country = addressRS.getString("country");
        city = addressRS.getString("city");
      }


      orgStmt = connection.prepareStatement(GET_ORGANIZATION_BY_UID);
      orgStmt.setInt(1, uid);
      orgRS = orgStmt.executeQuery();
      String organization = "";
      if (orgRS.next()) {
        organization = orgRS.getString("org_name");
      }

      return new ExpatUser(uid, username, userPassword, email, orcid, organization, country, city, salt);
    } finally {
      if (addressRS != null) {
        addressRS.close();
      }
      if (orgRS != null) {
        orgRS.close();
      }
      if (addressStmt != null) {
        addressStmt.close();
      }
      if (orgStmt != null) {
        orgStmt.close();
      }
    }
  }

  public void updateUserPassword(Connection connection,
                                 ExpatUser expatUser, String password, boolean dryRun) throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(UPDATE_PWD)) {
      stmt.setString(1, password);
      stmt.setInt(2, expatUser.getUid());

      if (dryRun) {
        LOGGER.log(Level.INFO, stmt.toString());
        return;
      }
      stmt.execute();
    }
  }

  public void updateMode(Connection connection,
                         ExpatUser expatUser, int mode, boolean dryRun) throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(UPDATE_MODE)) {
      stmt.setInt(1, mode);
      stmt.setInt(2, expatUser.getUid());

      if (dryRun) {
        LOGGER.log(Level.INFO, stmt.toString());
        return;
      }
      stmt.execute();
    }
  }
}
