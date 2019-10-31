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

package io.hops.hopsworks.expat.db;

import com.zaxxer.hikari.HikariDataSource;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.sql.Connection;
import java.sql.SQLException;

public class DbConnectionFactory {

  private static HikariDataSource ds = null;

  private static void init() throws ConfigurationException {
    Configuration config = ConfigurationBuilder.getConfiguration();

    ds = new HikariDataSource();
    ds.setDriverClassName(config.getString(ExpatConf.DATABASE_DBMS_DRIVER_NAME,
        ExpatConf.DATABASE_DBMS_DRIVER_NAME_DEFAULT));
    ds.setJdbcUrl(config.getString(ExpatConf.DATABASE_URL));
    ds.setUsername(config.getString(ExpatConf.DATABASE_USER_KEY));
    ds.setPassword(config.getString(ExpatConf.DATABASE_PASSWORD_KEY));
    ds.setReadOnly(config.getBoolean(ExpatConf.DRY_RUN));
  }

  public static Connection getConnection() throws ConfigurationException, SQLException {
    if (ds == null) {
      init();
    }
    return ds.getConnection();
  }
}
