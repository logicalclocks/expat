/**
 * This file is part of Expat
 * Copyright (C) 2019, Logical Clocks AB. All rights reserved
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
package io.hops.hopsworks.expat.migrations.projects.provenance;

import io.hops.hopsworks.common.hdfs.DistributedFileSystemOps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.security.UserGroupInformation;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumSet;

public class HopsClient {
  
  private final static String HDFS_ENDPOINT = "SELECT rpc_addresses FROM hops.hdfs_le_descriptors ";
  
  public static DistributedFileSystemOps getDFSO(Connection connection) throws SQLException {
    String hadoopHome = System.getenv("HADOOP_HOME");
    if(hadoopHome == null || hadoopHome == "") {
      throw new IllegalArgumentException("env HADOOP_HOME is not set");
    }
    String hadoopConfDir = hadoopHome + "/etc/hadoop";
    //Get the configuration file at found path
    File hadoopConfFile = new File(hadoopConfDir, "core-site.xml");
    if (!hadoopConfFile.exists()) {
      throw new IllegalStateException("No hadoop conf file: core-site.xml");
    }
    
    File hdfsConfFile = new File(hadoopConfDir, "hdfs-site.xml");
    if (!hdfsConfFile.exists()) {
      throw new IllegalStateException("No hdfs conf file: hdfs-site.xml");
    }
    
    Path hdfsPath = new Path(hdfsConfFile.getAbsolutePath());
    Path hadoopPath = new Path(hadoopConfFile.getAbsolutePath());
    
    Configuration conf = new Configuration();
    conf.addResource(hadoopPath);
    conf.addResource(hdfsPath);
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "0027");
    conf.setStrings("dfs.namenode.rpc-address", getRPCEndpoint(connection));
    UserGroupInformation superUser = UserGroupInformation.createRemoteUser("hdfs");
    DistributedFileSystemOps dfso = new DistributedFileSystemOps(superUser, conf);
    return dfso;
  }
  
  private static String getRPCEndpoint(Connection connection) throws SQLException, IllegalStateException {
    PreparedStatement hdfsEndpointStmt = null;
    try {
      hdfsEndpointStmt = connection.prepareStatement(HDFS_ENDPOINT);
      ResultSet hdfsEndpointResultSet = hdfsEndpointStmt.executeQuery();
      if (hdfsEndpointResultSet.next()) {
        //return first RPC - for migration we are not that interested in load balancing
        String rpcAddresses = hdfsEndpointResultSet.getString(1);
        rpcAddresses = rpcAddresses.trim();
        
        if(rpcAddresses.contains(",")) {
          String[] rpcAddressArr = rpcAddresses.split(",");
          return rpcAddressArr[0];
        } else {
          return rpcAddresses;
        }
      } else {
        throw new IllegalStateException("no hdfs rpc endpoint");
      }
    } finally {
      if(hdfsEndpointStmt != null) {
        hdfsEndpointStmt.close();
      }
    }
  }
  
  public static void upsertXAttr(DistributedFileSystemOps dfso, String path, String name, byte[] value)
    throws IOException {
    EnumSet<XAttrSetFlag> flags = EnumSet.noneOf(XAttrSetFlag.class);
    if(dfso.getXAttr(path, name) != null) {
      flags.add(XAttrSetFlag.REPLACE);
    } else {
      flags.add(XAttrSetFlag.CREATE);
    }
    dfso.setXAttr(path, name, value, flags);
  }
  
  public static void removeXAttr(DistributedFileSystemOps dfso, String path, String name) throws IOException {
    if(dfso.getXAttr(path, name) != null) {
      dfso.removeXAttr(path, name);
    }
  }
}
