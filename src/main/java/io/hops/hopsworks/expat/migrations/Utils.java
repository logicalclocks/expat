/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.hopsworks.expat.migrations;

import io.hops.hopsworks.common.util.SystemCommandExecutor;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Utils {
  private final static Logger LOGGER = Logger.getLogger(Utils.class.getName());
  
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
      LOGGER.log(Level.SEVERE, "Error while waiting for OpenSSL command to execute");
      throw new IOException(ex);
    }
  }
}
