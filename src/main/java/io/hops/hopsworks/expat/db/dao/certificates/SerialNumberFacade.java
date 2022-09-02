/**
 * This file is part of Expat
 * Copyright (C) 2022, Logical Clocks AB. All rights reserved
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

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class SerialNumberFacade {
  private static final Logger LOGGER = LogManager.getLogger(SerialNumberFacade.class);
  private static final String TABLE_NAME = "pki_serial_number";
  private static final String INIT_SERIAL_NUMBER = String.format("INSERT INTO %s VALUES(?, ?)", TABLE_NAME);
  private static final String GET_SERIAL_NUMBER = String.format("SELECT * FROM %s WHERE type = ?", TABLE_NAME);

  private final Connection connection;
  private final boolean dryRun;

  public SerialNumberFacade(Connection connection, boolean dryRun) {
    this.connection = connection;
    this.dryRun = dryRun;
  }

  public void initializeSerialNumber(String type, Long number) throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(INIT_SERIAL_NUMBER)) {
      stmt.setString(1, type.toUpperCase());
      stmt.setLong(2, number);

      if (dryRun) {
        LOGGER.log(Level.INFO, "Executing " + stmt);
      } else {
        stmt.execute();
      }
    }
  }

  public boolean exists(String type) throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(GET_SERIAL_NUMBER)) {
      stmt.setString(1, type);
      if (dryRun) {
        LOGGER.log(Level.INFO, "DryRun - Executing " + stmt);
        return false;
      } else {
        return stmt.execute();
      }
    }
  }

  public void truncate() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.executeQuery("TRUNCATE TABLE pki_serial_number");
    }
  }
}
