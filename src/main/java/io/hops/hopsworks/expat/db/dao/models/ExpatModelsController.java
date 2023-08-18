package io.hops.hopsworks.expat.db.dao.models;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;


public class ExpatModelsController {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExpatModelsController.class);

  private ExpatModelFacade modelFacade;

  public ExpatModelsController(Connection connection) {
    this.modelFacade = new ExpatModelFacade(ExpatModel.class, connection);
  }

  public ExpatModel getByProjectAndName(Integer projectId, String name) throws SQLException,
    IllegalAccessException, InstantiationException {
    return modelFacade.findByProjectAndName(projectId, name);
  }
}