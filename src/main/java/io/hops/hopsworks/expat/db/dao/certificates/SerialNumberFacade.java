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
  private static final String INIT_SERIAL_NUMBER = "INSERT INTO pki_serial_number VALUES(?, ?)";

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

  public void truncate() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.executeQuery("TRUNCATE TABLE pki_serial_number");
    }
  }
}
