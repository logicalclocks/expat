/**
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
package io.hops.hopsworks.expat.migrations;

import io.hops.hopsworks.common.util.SystemCommandExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;

public class Utils {
  private static final Logger LOGGER = LogManager.getLogger(Utils.class);
  public static String executeCommand(List<String> commands, boolean redirectErrorStream) throws IOException {
    SystemCommandExecutor commandExecutor = new SystemCommandExecutor(commands, redirectErrorStream);
    try {
      int returnValue = commandExecutor.executeCommand();
      String stdout = commandExecutor.getStandardOutputFromCommand().trim(); // Remove \n from the string
      String stderr = commandExecutor.getStandardErrorFromCommand().trim(); // Remove \n from the string
      if (returnValue != 0) {
        throw new IOException(stderr);
      }
      return stdout;
    } catch (InterruptedException ex) {
      LOGGER.error("Error while waiting for OpenSSL command to execute", ex);
      throw new IOException(ex);
    }
  }
}
