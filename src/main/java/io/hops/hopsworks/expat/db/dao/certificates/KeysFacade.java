package io.hops.hopsworks.expat.db.dao.certificates;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class KeysFacade {
  private static final Logger LOGGER = LogManager.getLogger(KeysFacade.class);
  private static final String INSERT_KEY = "INSERT INTO pki_key VALUES(?, ?, ?)";
  private final Connection connection;
  private final boolean dryRun;

  public KeysFacade(Connection connection, boolean dryRun) {
    this.connection = connection;
    this.dryRun = dryRun;
  }

  public void insertKey(String owner, Integer type, byte[] key) throws SQLException {
    try (PreparedStatement stmt = connection.prepareStatement(INSERT_KEY)) {
      stmt.setString(1, owner.toUpperCase());
      stmt.setInt(2, type);
      stmt.setBytes(3, key);

      if (dryRun) {
        LOGGER.log(Level.INFO, "Executing " + stmt);
      } else {
        stmt.execute();
      }
    }
  }

  public void truncate() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.executeQuery("TRUNCATE TABLE pki_key");
    }
  }
}
