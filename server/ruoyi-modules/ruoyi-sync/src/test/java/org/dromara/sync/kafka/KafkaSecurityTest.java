package org.dromara.sync.kafka;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.sync.domain.DataSource;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The Kafka client security of a data source (SASL PLAIN / SCRAM, SSL) and its broker list. */
@Tag("dev")
class KafkaSecurityTest {

    private static final String CERT = "-----BEGIN CERTIFICATE-----\nMIIBcert\n-----END CERTIFICATE-----";
    private static final String KEY = "-----BEGIN PRIVATE KEY-----\nMIIEkey\n-----END PRIVATE KEY-----";

    @Test
    void aPlaintextClusterCarriesNoSecurityAndKeepsNothingBehind() {
        DataSource source = kafka(null);
        assertEquals(Map.of(), KafkaSecurity.clientProperties(source));
        assertEquals(KafkaConnection.plaintext("broker.example:9092"), KafkaConnection.of(source));

        source.setKafkaSecurityProtocol("plaintext");
        source.setKafkaSaslMechanism("PLAIN");
        source.setKafkaSslCaCert(CERT);
        source.setKafkaSslVerifyHostname("0");
        KafkaSecurity.normalize(source);
        assertNull(source.getKafkaSecurityProtocol());
        assertNull(source.getKafkaSaslMechanism());
        assertNull(source.getKafkaSslCaCert());
        assertNull(source.getKafkaSslVerifyHostname());
    }

    @Test
    void saslPlainAuthenticatesWithTheUsernameAndPassword() {
        DataSource source = kafka("sasl_plaintext");
        source.setKafkaSaslMechanism("plain");
        source.setUsername("sync");
        source.setPassword("p\"a\\ss");
        KafkaSecurity.normalize(source);

        assertEquals("SASL_PLAINTEXT", source.getKafkaSecurityProtocol());
        assertEquals("PLAIN", source.getKafkaSaslMechanism());
        Map<String, String> properties = KafkaSecurity.clientProperties(source);
        assertEquals(List.of("security.protocol", "sasl.mechanism", "sasl.jaas.config"), List.copyOf(properties.keySet()));
        assertEquals("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"sync\" password=\"p\\\"a\\\\ss\";",
            properties.get("sasl.jaas.config"));
    }

    @Test
    void scramOverSslTrustsTheGivenCaAndMayTurnTheHostNameCheckOff() {
        DataSource source = kafka("SASL_SSL");
        source.setKafkaSaslMechanism("SCRAM-SHA-512");
        source.setUsername("sync");
        source.setPassword("secret");
        source.setKafkaSslCaCert("  " + CERT.replace("\n", "\r\n") + "\r\n");
        source.setKafkaSslVerifyHostname("0");
        KafkaSecurity.normalize(source);

        assertEquals(CERT, source.getKafkaSslCaCert(), "trimmed, Unix line ends");
        Map<String, String> properties = KafkaSecurity.clientProperties(source);
        assertEquals("SASL_SSL", properties.get("security.protocol"));
        assertEquals("SCRAM-SHA-512", properties.get("sasl.mechanism"));
        assertTrue(properties.get("sasl.jaas.config").startsWith("org.apache.kafka.common.security.scram.ScramLoginModule required "));
        assertEquals("PEM", properties.get("ssl.truststore.type"));
        assertEquals(CERT, properties.get("ssl.truststore.certificates"));
        assertEquals("", properties.get("ssl.endpoint.identification.algorithm"));
        assertFalse(properties.containsKey("ssl.keystore.key"));
    }

    @Test
    void sslWithAClientCertificateAuthenticatesTheClientAndChecksHostNamesByDefault() {
        DataSource source = kafka("SSL");
        source.setKafkaSslClientCert(CERT);
        source.setKafkaSslClientKey(KEY);
        source.setKafkaSaslMechanism("PLAIN");
        KafkaSecurity.normalize(source);

        assertNull(source.getKafkaSaslMechanism(), "SSL alone uses no SASL mechanism");
        assertEquals("1", source.getKafkaSslVerifyHostname());
        Map<String, String> properties = KafkaSecurity.clientProperties(source);
        assertEquals(List.of("security.protocol", "ssl.keystore.type", "ssl.keystore.certificate.chain", "ssl.keystore.key"),
            List.copyOf(properties.keySet()));
        assertEquals(KEY, properties.get("ssl.keystore.key"));
    }

    @Test
    void anIncompleteOrMalformedSetupIsRefusedBeforeItIsSaved() {
        assertRefused(kafka("KERBEROS"), "安全协议");

        DataSource noMechanism = kafka("SASL_SSL");
        noMechanism.setUsername("sync");
        noMechanism.setPassword("secret");
        assertRefused(noMechanism, "SASL 认证机制");

        DataSource noPassword = kafka("SASL_PLAINTEXT");
        noPassword.setKafkaSaslMechanism("SCRAM-SHA-256");
        noPassword.setUsername("sync");
        noPassword.setPassword("");
        assertRefused(noPassword, "用户名和密码");

        DataSource badCa = kafka("SSL");
        badCa.setKafkaSslCaCert("not a certificate");
        assertRefused(badCa, "CA 证书");

        DataSource certWithoutKey = kafka("SSL");
        certWithoutKey.setKafkaSslClientCert(CERT);
        assertRefused(certWithoutKey, "同时填写");

        DataSource pkcs1 = kafka("SSL");
        pkcs1.setKafkaSslClientCert(CERT);
        pkcs1.setKafkaSslClientKey("-----BEGIN RSA PRIVATE KEY-----\nMIIE\n-----END RSA PRIVATE KEY-----");
        assertRefused(pkcs1, "PKCS#8");
    }

    @Test
    void connectionsDifferByTheirCredentialsButNeverPrintThem() {
        DataSource first = kafka("SASL_PLAINTEXT");
        first.setKafkaSaslMechanism("PLAIN");
        first.setUsername("sync");
        first.setPassword("first-secret");
        DataSource second = kafka("SASL_PLAINTEXT");
        second.setKafkaSaslMechanism("PLAIN");
        second.setUsername("sync");
        second.setPassword("second-secret");

        assertNotEquals(KafkaConnection.of(first), KafkaConnection.of(second));
        assertEquals("broker.example:9092 (SASL_PLAINTEXT)", KafkaConnection.of(first).toString());
        assertEquals("first-secret", KafkaConnection.of(first).clientProperties().getProperty("sasl.jaas.config").replaceAll(".*password=\"([^\"]*)\".*", "$1"));
    }

    @Test
    void aBrokerListTakesTheDataSourcePortWhereAnEntryNamesNone() {
        DataSource source = kafka(null);
        assertEquals("broker.example:9092", KafkaAdminClients.bootstrapServers(source), "a single host as it always was");

        source.setHost(" k1:9093, k2 ,k3:19092,[::1],[fe80::1]:9094 ");
        assertEquals("k1:9093,k2:9092,k3:19092,[::1]:9092,[fe80::1]:9094", KafkaAdminClients.bootstrapServers(source));
        assertEquals(List.of("K1/9093", "K2/9092", "K3/19092", "[::1]/9092", "[FE80::1]/9094"),
            KafkaAdminClients.brokers(source, (host, port) -> host.toUpperCase() + '/' + port));
    }

    @Test
    void aMalformedBrokerListIsRefused() {
        for (String host : new String[]{"k1:abc", "k1:0", "k1:70000", ":9092", " , "}) {
            DataSource source = kafka(null);
            source.setHost(host);
            assertThrows(ServiceException.class, () -> KafkaAdminClients.bootstrapServers(source), host);
        }
    }

    private static void assertRefused(DataSource source, String expected) {
        String message = assertThrows(ServiceException.class, () -> KafkaSecurity.normalize(source)).getMessage();
        assertTrue(message.contains(expected), message);
    }

    private static DataSource kafka(String protocol) {
        DataSource source = new DataSource();
        source.setSourceType("KAFKA");
        source.setHost("broker.example");
        source.setPort(9092);
        source.setKafkaSecurityProtocol(protocol);
        return source;
    }
}
