package io.hops.hopsworks.expat.db.dao.certificates;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class CRLFacade {
  private static final Logger LOGGER = LogManager.getLogger(CRLFacade.class);
  private static final String INSERT_CRL = "INSERT INTO pki_crl VALUES(?, ?)";

  private final Connection connection;
  private final boolean dryRun;

  public CRLFacade(Connection connection, boolean dryRun) {
    this.connection = connection;
    this.dryRun = dryRun;
  }

  public void insertCRL(String type, byte[] crl) throws SQLException  {
    try (PreparedStatement stmt = connection.prepareStatement(INSERT_CRL)) {
      stmt.setString(1, type.toUpperCase());
      stmt.setBytes(2, crl);

      if (dryRun) {
        LOGGER.log(Level.INFO, "Executing: " + stmt);
      } else {
        stmt.execute();
      }
    }
  }

  public void truncate() throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.executeQuery("TRUNCATE TABLE pki_crl");
    }
  }
}
