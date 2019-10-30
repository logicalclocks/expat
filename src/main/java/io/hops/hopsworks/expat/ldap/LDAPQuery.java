/*
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

package io.hops.hopsworks.expat.ldap;

import io.hops.hopsworks.expat.db.dao.user.ExpatUser;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import net.sf.michaelo.dirctxsrc.DirContextSource;
import org.apache.commons.configuration2.Configuration;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

public class LDAPQuery {

  private DirContext ctx = null;

  private static final String OBJECTGUID_KEY = "objectguid";

  private static final String[] DN_ONLY = {"dn", OBJECTGUID_KEY};
  private static final String LDAP_ATTR_BINARY = "java.naming.ldap.attributes.binary";

  private String baseDN = "";

  public LDAPQuery(Configuration config) throws NamingException {
    baseDN = config.getString(ExpatConf.LDAP_BASE_DN_KEY);

    // Set up the environment for creating the initial context
    DirContextSource.Builder builder = new DirContextSource.Builder(config.getString(ExpatConf.LDAP_URL));
    builder.additionalProperty(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
        .additionalProperty(LDAP_ATTR_BINARY, "objectGUID")
        .gssApiAuth("com.logicalclocks.expat");

    ctx = builder.build().getDirContext();
  }

  private String prefixZeros(int value) {
    if (value <= 0xF) {
      return "0" + Integer.toHexString(value);
    } else {
      return Integer.toHexString(value);
    }
  }

  private String convertToDashedString(byte[] objectGUID) {
    return prefixZeros((int) objectGUID[3] & 0xFF) +
        prefixZeros((int) objectGUID[2] & 0xFF) +
        prefixZeros((int) objectGUID[1] & 0xFF) +
        prefixZeros((int) objectGUID[0] & 0xFF) +
        "-" +
        prefixZeros((int) objectGUID[5] & 0xFF) +
        prefixZeros((int) objectGUID[4] & 0xFF) +
        "-" +
        prefixZeros((int) objectGUID[7] & 0xFF) +
        prefixZeros((int) objectGUID[6] & 0xFF) +
        "-" +
        prefixZeros((int) objectGUID[8] & 0xFF) +
        prefixZeros((int) objectGUID[9] & 0xFF) +
        "-" +
        prefixZeros((int) objectGUID[10] & 0xFF) +
        prefixZeros((int) objectGUID[11] & 0xFF) +
        prefixZeros((int) objectGUID[12] & 0xFF) +
        prefixZeros((int) objectGUID[13] & 0xFF) +
        prefixZeros((int) objectGUID[14] & 0xFF) +
        prefixZeros((int) objectGUID[15] & 0xFF);
  }

  private String getUUIDAttribute(Attributes attrs, String key) throws NamingException {
    Attribute attr = attrs.remove(key);
    byte[] guid = attr != null ? (byte[]) attr.get() : "".getBytes();
    return convertToDashedString(guid);
  }

  private String getEmailFilter(ExpatUser expatUser) {
    return "mail=" + expatUser.getEmail();
  }

  public String getUUID(ExpatUser expatUser) throws NamingException, LdapUserNotFound {
    SearchControls ctls = new SearchControls();
    ctls.setReturningAttributes(DN_ONLY);
    ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);
    ctls.setCountLimit(1);

    NamingEnumeration answer = ctx.search(baseDN, getEmailFilter(expatUser), ctls);
    if (answer.hasMore()) {
      SearchResult res = (SearchResult) answer.next();
      return getUUIDAttribute(res.getAttributes(), OBJECTGUID_KEY);
    } else {
      throw new LdapUserNotFound();
    }
  }
}
