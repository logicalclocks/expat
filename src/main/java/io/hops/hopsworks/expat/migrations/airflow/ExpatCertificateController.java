package io.hops.hopsworks.expat.migrations.airflow;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.io.Files;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.hops.hopsworks.common.security.CSR;
import io.hops.hopsworks.common.util.HopsUtils;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.dao.util.ExpatVariables;
import io.hops.hopsworks.expat.db.dao.util.ExpatVariablesFacade;
import io.hops.hopsworks.expat.migrations.MigrationException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;
import org.bouncycastle.util.io.pem.PemObject;
import org.javatuples.Pair;


import java.io.IOException;
import java.io.StringWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;

import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.Security;
import java.security.PrivateKey;
import java.security.KeyStoreException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static io.hops.hopsworks.common.util.Settings.CERT_PASS_SUFFIX;
import static io.hops.hopsworks.common.util.Settings.KEYSTORE_SUFFIX;
import static io.hops.hopsworks.common.util.Settings.TRUSTSTORE_SUFFIX;

public class ExpatCertificateController {
  private static final Logger LOGGER = LogManager.getLogger(ExpatCertificateController.class);
  private ExpatVariablesFacade expatVariablesFacade;
  private KeyPairGenerator keyPairGenerator;
  private CertificateFactory certificateFactory;
  private CloseableHttpClient httpClient;
  private HttpHost httpHost;
  private final Connection connection;
  private static PoolingHttpClientConnectionManager httpConnectionManager;
  private final static String SECURITY_PROVIDER = "BC";
  private final static String KEY_ALGORITHM = "RSA";
  private final static String SIGNATURE_ALGORITHM = "SHA256withRSA";
  private final static String CERTIFICATE_TYPE = "X.509";
  private final static int KEY_SIZE = 2048;
  private ObjectMapper objectMapper;
  private String masterJwt;
  private String masterPassword;


  public void init() throws ConfigurationException, MigrationException, NoSuchAlgorithmException,
      KeyStoreException, KeyManagementException, SQLException, IOException {
    Configuration config = ConfigurationBuilder.getConfiguration();
    httpHost = HttpHost.create(config.getString(ExpatConf.HOPSWORKS_URL));
    httpConnectionManager = new PoolingHttpClientConnectionManager();
    httpConnectionManager.setDefaultMaxPerRoute(5);
    httpClient = HttpClients.custom().setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(
            CookieSpecs.IGNORE_COOKIES).build())
        .setSSLContext(new SSLContextBuilder().loadTrustMaterial((x509Certificates, s) -> true).build())
        .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
        .build();
    Security.addProvider(new BouncyCastleProvider());
    try {
      keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM, SECURITY_PROVIDER);
      keyPairGenerator.initialize(KEY_SIZE);

      certificateFactory = CertificateFactory.getInstance(CERTIFICATE_TYPE);
    } catch (Exception e) {
      LOGGER.error("Could not initialize the key generator", e);
    }
    masterJwt = getMasterServiceJWT();
    if (Strings.isNullOrEmpty(masterJwt)) {
      throw new MigrationException("Master service jwt cannot be null");
    }
    java.nio.file.Path masterPwdPath = Paths.get(config.getString(ExpatConf.MASTER_PWD_FILE_KEY));
    masterPassword = Files.toString(masterPwdPath.toFile(), Charset.defaultCharset());
    objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    objectMapper.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
    expatVariablesFacade = new ExpatVariablesFacade(ExpatVariables.class, connection);
  }

  public ExpatCertificateController(Connection connection) throws MigrationException {
    this.connection = connection;
    try {
      init();
    } catch (Exception e) {
      throw new MigrationException("Failed to initialize bean", e);
    }
  }

  public Pair<KeyStore, KeyStore> generateStores(String CN, String userKeyPwd) throws MigrationException {
    try {
      LOGGER.info("Generating keypair for " + CN);
      // Generate keypair
      KeyPair keyPair = keyPairGenerator.generateKeyPair();

      CSR csr = generateCSR(CN, keyPair);
      LOGGER.info("Sending Certificate Signing Request for " + CN);
      CSR signedCsr = signCSR(csr);
      LOGGER.info("Gotten signed certificate for " + CN);
      return buildStores(CN, userKeyPwd, keyPair.getPrivate(), signedCsr);

    } catch (OperatorCreationException | IOException e) {
      throw new MigrationException("Failed to generate stores", e);
    }
  }

  private CSR generateCSR(String cn, KeyPair keyPair) throws OperatorCreationException, IOException {
    // Generate CSR
    X500Name subject = new X500NameBuilder(BCStyle.INSTANCE)
        .addRDN(BCStyle.CN, cn)
        .build();
    PKCS10CertificationRequest csr = new JcaPKCS10CertificationRequestBuilder(subject, keyPair.getPublic())
        .build(new JcaContentSignerBuilder(SIGNATURE_ALGORITHM)
            .setProvider(SECURITY_PROVIDER)
            .build(keyPair.getPrivate()));

    // Stringfiy the csr so that it can be sent as json payload
    PemObject pemObject = new PemObject("CERTIFICATE REQUEST", csr.getEncoded());
    StringWriter csrSTR = new StringWriter();
    JcaPEMWriter jcaPEMWriter = new JcaPEMWriter(csrSTR);
    jcaPEMWriter.writeObject(pemObject);
    jcaPEMWriter.close();
    csrSTR.close();
    return new CSR(csrSTR.toString());
  }

  private CSR signCSR(CSR csr) throws MigrationException, IOException {
    String csrJSON = objectMapper.writeValueAsString(csr);
    CloseableHttpResponse response = null;
    try {
      HttpPost request = new HttpPost("/hopsworks-ca/v2/certificate/project");
      request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      request.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + masterJwt);
      request.setEntity(new StringEntity(csrJSON));
      response = httpClient.execute(httpHost, request);
      int status = response.getStatusLine().getStatusCode();
      if (status / 100 != 2) {
        throw new MigrationException("Failed to sign CSR with status: " + status + ". " + response.toString());
      }
      String responseJSON = EntityUtils.toString(response.getEntity(), Charset.defaultCharset());
      return objectMapper.readValue(responseJSON, CSR.class);
    } catch (IOException e) {
      throw new MigrationException("Can not sign CSR: " + e.getMessage());
    }finally {
      if (response != null) {
        response.close();
      }
    }
  }

  private Pair<KeyStore, KeyStore> buildStores(String CN, String userKeyPwd,
                                               PrivateKey privateKey, CSR signedCert) throws MigrationException {
    KeyStore keyStore = null;
    KeyStore trustStore = null;
    try {
      X509Certificate certificate = (X509Certificate) certificateFactory
          .generateCertificate(new ByteArrayInputStream(signedCert.getSignedCert().getBytes()));
      X509Certificate issuer = (X509Certificate) certificateFactory
          .generateCertificate(new ByteArrayInputStream(signedCert.getIntermediateCaCert().getBytes()));
      X509Certificate rootCa = (X509Certificate) certificateFactory
          .generateCertificate(new ByteArrayInputStream(signedCert.getRootCaCert().getBytes()));

      keyStore = KeyStore.getInstance("JKS");
      keyStore.load(null, null);
      X509Certificate[] chain = new X509Certificate[2];
      chain[0] = certificate;
      chain[1] = issuer;
      keyStore.setKeyEntry(CN, privateKey, userKeyPwd.toCharArray(), chain);

      trustStore = KeyStore.getInstance("JKS");
      trustStore.load(null, null);
      trustStore.setCertificateEntry("hops_root_ca", rootCa);
    } catch (IOException | CertificateException | KeyStoreException | NoSuchAlgorithmException e) {
      throw new MigrationException("Certificate creation error", e);
    }

    return new Pair<>(keyStore, trustStore);
  }

  public byte[] convertKeystoreToByteArray(KeyStore keyStore, String password)
      throws IOException, CertificateException, NoSuchAlgorithmException, KeyStoreException {
    ByteArrayOutputStream keyStoreStream = new ByteArrayOutputStream();
    keyStore.store(keyStoreStream, password.toCharArray());
    return keyStoreStream.toByteArray();
  }

  public String getMasterServiceJWT() throws SQLException {
    String jwt = null;
    try {
      ExpatVariables var = expatVariablesFacade.findById("service_master_jwt");
      jwt = var.getValue();
    } catch (IllegalAccessException | InstantiationException e) {
      LOGGER.error("Failed to get service_master_jwt", e);
    }
    return jwt;
  }

  public void createCertsSecretForUser(KubernetesClient kubernetesClient, String projectName, String username) {
    String nsName = projectName.toLowerCase().replaceAll("[^a-z0-9-]", "-");
    String kubeUsername = nsName + "--" + username.toLowerCase().replaceAll("[^a-z0-9]", "-");
    if (kubeUsername.endsWith("-")) {
      kubeUsername = kubeUsername + "0";
    }
    String hopsUsername = projectName + "__" + username;
    try {
      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery("SELECT password, user_key, " +
          "user_cert, user_key_pwd FROM users u join user_certs uc ON u.username = uc.username " +
          "AND u.username = '" + username + "' AND projectname = '" + projectName + "'");
      while (resultSet.next()) {
        String certPwd = HopsUtils.decrypt(resultSet.getString("password"),
            resultSet.getString("user_key_pwd"), masterPassword);
        Map<String, String> secretData = new HashMap<>();
        secretData.put(hopsUsername + CERT_PASS_SUFFIX, Base64.getEncoder().encodeToString(certPwd.getBytes()));
        secretData.put(hopsUsername + KEYSTORE_SUFFIX,
            Base64.getEncoder().encodeToString(resultSet.getBytes("user_key")));
        secretData.put(hopsUsername + TRUSTSTORE_SUFFIX,
            Base64.getEncoder().encodeToString(resultSet.getBytes("user_cert")));

        Secret secret = new SecretBuilder()
            .withMetadata(new ObjectMetaBuilder()
                .withName(kubeUsername)
                .build())
            .withData(secretData)
            .build();

        // Send request
        kubernetesClient.secrets().inNamespace(nsName).createOrReplace(secret);
        LOGGER.info("Secret " + kubeUsername + " created for project user: " + projectName);
      }
    } catch (Exception e) {
      LOGGER.error("Could not create secret " + kubeUsername + " for project user: "
          + projectName, e);
    }
  }
}
