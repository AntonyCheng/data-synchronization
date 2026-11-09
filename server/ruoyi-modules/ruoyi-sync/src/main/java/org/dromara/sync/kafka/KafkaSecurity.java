package org.dromara.sync.kafka;

import org.dromara.common.core.exception.ServiceException;
import org.dromara.common.core.utils.StringUtils;
import org.dromara.sync.domain.DataSource;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * How the platform authenticates to a Kafka data source: SASL with the PLAIN or SCRAM mechanisms,
 * SSL (server certificate check, optionally a client certificate), or both - as the Kafka client
 * properties every client of that cluster carries (AdminClient, the bridge's consumer and producer,
 * the engine's Kafka sink).
 *
 * <p>A data source without a protocol is PLAINTEXT and yields no properties at all, so the clients -
 * and the engine configs, and through them the config fingerprints - of every Kafka data source
 * from before this existed stay exactly as they were.
 *
 * <p>SASL authenticates with the data source's username and password. Certificates and the client
 * key are PEM text kept on the data source, so nothing has to be mounted into the engine or backend
 * containers; the client key is stored encrypted like the password and never returned.
 */
public final class KafkaSecurity {

    public static final String PLAINTEXT = "PLAINTEXT";
    public static final String SASL_PLAINTEXT = "SASL_PLAINTEXT";
    public static final String SASL_SSL = "SASL_SSL";
    public static final String SSL = "SSL";
    public static final List<String> PROTOCOLS = List.of(PLAINTEXT, SASL_PLAINTEXT, SASL_SSL, SSL);

    public static final String PLAIN = "PLAIN";
    public static final String SCRAM_SHA_256 = "SCRAM-SHA-256";
    public static final String SCRAM_SHA_512 = "SCRAM-SHA-512";
    public static final List<String> MECHANISMS = List.of(PLAIN, SCRAM_SHA_256, SCRAM_SHA_512);

    private static final String PLAIN_LOGIN_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule";
    private static final String SCRAM_LOGIN_MODULE = "org.apache.kafka.common.security.scram.ScramLoginModule";
    private static final String CERTIFICATE_BEGIN = "-----BEGIN CERTIFICATE-----";
    private static final String PKCS8_KEY_BEGIN = "-----BEGIN PRIVATE KEY-----";

    private KafkaSecurity() {
    }

    /** The data source's security protocol; blank is PLAINTEXT. */
    public static String protocol(DataSource source) {
        String protocol = source == null ? null : source.getKafkaSecurityProtocol();
        return StringUtils.isBlank(protocol) ? PLAINTEXT : protocol.trim().toUpperCase(Locale.ROOT);
    }

    public static boolean usesSasl(String protocol) {
        return SASL_PLAINTEXT.equals(protocol) || SASL_SSL.equals(protocol);
    }

    public static boolean usesSsl(String protocol) {
        return SSL.equals(protocol) || SASL_SSL.equals(protocol);
    }

    /**
     * The Kafka client properties of the data source, in a fixed order; empty for PLAINTEXT. They carry
     * the credentials (the SASL JAAS config, the client key) - never log or return them.
     */
    public static Map<String, String> clientProperties(DataSource source) {
        Map<String, String> properties = new LinkedHashMap<>();
        String protocol = protocol(source);
        if (PLAINTEXT.equals(protocol)) return properties;
        properties.put("security.protocol", protocol);
        if (usesSasl(protocol)) {
            String mechanism = source.getKafkaSaslMechanism() == null ? "" : source.getKafkaSaslMechanism().trim().toUpperCase(Locale.ROOT);
            properties.put("sasl.mechanism", mechanism);
            properties.put("sasl.jaas.config", (PLAIN.equals(mechanism) ? PLAIN_LOGIN_MODULE : SCRAM_LOGIN_MODULE)
                + " required username=\"" + jaasValue(source.getUsername()) + "\" password=\"" + jaasValue(source.getPassword()) + "\";");
        }
        if (usesSsl(protocol)) {
            if (StringUtils.isNotBlank(source.getKafkaSslCaCert())) {
                properties.put("ssl.truststore.type", "PEM");
                properties.put("ssl.truststore.certificates", pem(source.getKafkaSslCaCert()));
            }
            if (StringUtils.isNotBlank(source.getKafkaSslClientCert()) && StringUtils.isNotBlank(source.getKafkaSslClientKey())) {
                properties.put("ssl.keystore.type", "PEM");
                properties.put("ssl.keystore.certificate.chain", pem(source.getKafkaSslClientCert()));
                properties.put("ssl.keystore.key", pem(source.getKafkaSslClientKey()));
            }
            // Kafka checks the broker's host name against its certificate unless this is empty.
            if ("0".equals(source.getKafkaSslVerifyHostname())) properties.put("ssl.endpoint.identification.algorithm", "");
        }
        return properties;
    }

    /**
     * Normalizes the security fields of a Kafka data source and refuses an incomplete or malformed
     * setup before it is saved or tested. Fields a protocol does not use are cleared, so switching
     * back to PLAINTEXT leaves nothing behind. SASL needs the username and password: on an edit the
     * caller has already put the stored password (and client key) back when the form left them blank.
     */
    public static void normalize(DataSource source) {
        String protocol = protocol(source);
        if (!PROTOCOLS.contains(protocol)) {
            throw new ServiceException("Kafka 安全协议只支持 PLAINTEXT、SASL_PLAINTEXT、SASL_SSL、SSL");
        }
        source.setKafkaSecurityProtocol(PLAINTEXT.equals(protocol) ? null : protocol);
        if (usesSasl(protocol)) {
            String mechanism = source.getKafkaSaslMechanism() == null ? "" : source.getKafkaSaslMechanism().trim().toUpperCase(Locale.ROOT);
            if (!MECHANISMS.contains(mechanism)) {
                throw new ServiceException("SASL 认证机制只支持 PLAIN、SCRAM-SHA-256、SCRAM-SHA-512");
            }
            if (StringUtils.isBlank(source.getUsername()) || StringUtils.isBlank(source.getPassword())) {
                throw new ServiceException("SASL 认证需要填写用户名和密码");
            }
            source.setKafkaSaslMechanism(mechanism);
        } else {
            source.setKafkaSaslMechanism(null);
        }
        if (usesSsl(protocol)) {
            source.setKafkaSslCaCert(blankToNull(pem(source.getKafkaSslCaCert())));
            source.setKafkaSslClientCert(blankToNull(pem(source.getKafkaSslClientCert())));
            source.setKafkaSslClientKey(blankToNull(pem(source.getKafkaSslClientKey())));
            if (source.getKafkaSslCaCert() != null && !source.getKafkaSslCaCert().contains(CERTIFICATE_BEGIN)) {
                throw new ServiceException("CA 证书须为 PEM 格式（以 " + CERTIFICATE_BEGIN + " 开头）");
            }
            if ((source.getKafkaSslClientCert() == null) != (source.getKafkaSslClientKey() == null)) {
                throw new ServiceException("客户端证书和客户端私钥须同时填写（双向认证），或都留空");
            }
            if (source.getKafkaSslClientCert() != null && !source.getKafkaSslClientCert().contains(CERTIFICATE_BEGIN)) {
                throw new ServiceException("客户端证书须为 PEM 格式（以 " + CERTIFICATE_BEGIN + " 开头）");
            }
            if (source.getKafkaSslClientKey() != null && !source.getKafkaSslClientKey().contains(PKCS8_KEY_BEGIN)) {
                throw new ServiceException("客户端私钥须为未加密的 PKCS#8 PEM（以 " + PKCS8_KEY_BEGIN + " 开头）；"
                    + "RSA / EC 格式或带口令的私钥可用 openssl pkcs8 -topk8 -nocrypt -in client.key -out client-pkcs8.key 转换");
            }
            String verify = source.getKafkaSslVerifyHostname();
            source.setKafkaSslVerifyHostname("0".equals(verify) ? "0" : "1");
        } else {
            source.setKafkaSslCaCert(null);
            source.setKafkaSslClientCert(null);
            source.setKafkaSslClientKey(null);
            source.setKafkaSslVerifyHostname(null);
        }
    }

    /** A value inside a JAAS config's double quotes: backslash and quote escaped. */
    private static String jaasValue(String value) {
        return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /** PEM text as pasted: trimmed, with Unix line ends. */
    private static String pem(String value) {
        return value == null ? null : value.replace("\r\n", "\n").replace('\r', '\n').trim();
    }

    private static String blankToNull(String value) {
        return StringUtils.isBlank(value) ? null : value;
    }
}
