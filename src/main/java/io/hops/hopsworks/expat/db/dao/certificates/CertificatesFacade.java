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

import io.hops.hopsworks.expat.db.dao.user.ExpatUser;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class CertificatesFacade {

  private static final Logger LOGGER = LogManager.getLogger(CertificatesFacade.class);

  private static final String UPDATE_PWD =
      "UPDATE user_certs SET user_key_pwd=? WHERE projectname=? AND username=?";
  private static final String GET_USER_CERT =
      "SELECT * from user_certs WHERE username = ?";

  public void updateCertPassword(Connection connection,
                                 ExpatCertificate expatCertificate, String newPassword, boolean dryRun)
      throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(UPDATE_PWD)) {

      stmt.setString(1, newPassword);
      stmt.setString(2, expatCertificate.getProjectName());
      stmt.setString(3, expatCertificate.getUsername());

      if (dryRun) {
        LOGGER.log(Level.INFO, stmt.toString());
        return;
      }
      stmt.execute();
    }
  }

  public List<ExpatCertificate> getUserCertificates(Connection connection, ExpatUser expatUser) throws SQLException {
    ResultSet rs = null;
    List<ExpatCertificate> certificates = new ArrayList<>();
    try (PreparedStatement stmt = connection.prepareStatement(GET_USER_CERT)) {
      stmt.setString(1, expatUser.getUsername());
      rs = stmt.executeQuery();
      while (rs.next()) {
        certificates.add(new ExpatCertificate(
            rs.getString("projectname"),
            rs.getString("username"),
            rs.getString("user_key_pwd")));
      }
    } finally {
      if (rs != null) {
        rs.close();
      }
    }

    return certificates;
  }
}
