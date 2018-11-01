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

package io.hops.hopsworks.expat.configuration;

public class ExpatConf {
  // ------ Database Configuration ------ //
  private static final String DATABASE_PREFIX = "database.";
  public static final String DATABASE_DBMS_DRIVER_NAME = DATABASE_PREFIX + "driver";
  public static final String DATABASE_DBMS_DRIVER_NAME_DEFAULT = "com.mysql.jdbc.Driver";

  public static final String DATABASE_URL = DATABASE_PREFIX + "url";
  public static final String DATABASE_USER_KEY = DATABASE_PREFIX + "user";
  public static final String DATABASE_PASSWORD_KEY = DATABASE_PREFIX + "password";

  // ------ Kubernetes Configuration ------ //
  private static final String KUBE_PREFIX = "kube.";
  public static final String KUBE_USER_KEY = KUBE_PREFIX + "user";
  public static final String KUBE_MASTER_URL_KEY = KUBE_PREFIX + "masterUrl";
  public static final String KUBE_CA_CERTFILE_KEY = KUBE_PREFIX + "caPath";
  public static final String KUBE_TSTORE_PATH_KEY = KUBE_PREFIX + "tstorePath";
  public static final String KUBE_TSTORE_PWD_KEY= KUBE_PREFIX + "tstorePwd";
  public static final String KUBE_KSTORE_PATH_KEY = KUBE_PREFIX + "kstorePath";
  public static final String KUBE_KSTORE_PWD_KEY = KUBE_PREFIX + "tstorePwd";

  public static final String KUBE_CERTFILE_KEY = KUBE_PREFIX + "certFile";
  public static final String KUBE_KEYFILE_KEY = KUBE_PREFIX + "keyFile";
  public static final String KUBE_KEYPWD_KEY = KUBE_PREFIX + "keyPwd";

  // ------- X.509 configuration -------- //
  private static final String CERTS_PREFIX = "x509.";
  public static final String MASTER_PWD_FILE_KEY = CERTS_PREFIX + "masterPwdFile";
  public static final String INTERMEDIATE_CA_PATH = CERTS_PREFIX + "intermediateCA";
  public static final String CREATE_USER_CERT_SCRIPT = CERTS_PREFIX + "userCertsScript";
}
