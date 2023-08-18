/*
 * This file is part of Expat
 * Copyright (C) 2020, Logical Clocks AB. All rights reserved
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
package io.hops.hopsworks.expat.db.dao.models;

import io.hops.hopsworks.expat.db.dao.ExpatAbstractFacade;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.List;

public class ExpatModelFacade extends ExpatAbstractFacade<ExpatModel> {
  private static final String FIND_BY_PROJECT_AND_NAME = "SELECT * FROM hopsworks.model " +
    "WHERE project_id = ? AND name = ?";
  private Connection connection;
  protected ExpatModelFacade(Class<ExpatModel> entityClass) {
    super(entityClass);
  }

  public ExpatModelFacade(Class<ExpatModel> entityClass, Connection connection) {
    super(entityClass);
    this.connection = connection;
  }

  @Override
  public Connection getConnection() {
    return this.connection;
  }

  @Override
  public String findAllQuery() {
    return "SELECT * FROM hopsworks.model";
  }

  @Override
  public String findByIdQuery() {
    return "SELECT * FROM hopsworks.model WHERE id = ?";
  }

  public ExpatModel findByProjectAndName(Integer projectId, String name)
    throws IllegalAccessException, SQLException, InstantiationException {
    List<ExpatModel> hdfsGroupList = this.findByQuery(FIND_BY_PROJECT_AND_NAME, new Object[]{projectId, name},
      new JDBCType[]{JDBCType.INTEGER, JDBCType.VARCHAR});
    if (hdfsGroupList.isEmpty()) {
      return null;
    }
    if (hdfsGroupList.size() > 1) {
      throw new IllegalStateException("More than one results found");
    }
    return hdfsGroupList.get(0);
  }
}
