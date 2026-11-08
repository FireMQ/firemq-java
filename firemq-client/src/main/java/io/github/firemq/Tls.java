package io.github.firemq;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;

/** Wraps a connected TCP socket in TLS according to the connect options. */
final class Tls {
    private Tls() {}

    static SSLSocket wrap(Socket tcp, ConnectOptions opts, int timeoutMillis) throws IOException {
        SSLContext ctx;
        try {
            ctx = opts.sslContext() != null ? opts.sslContext() : context(opts);
        } catch (GeneralSecurityException e) {
            throw new FireMQException("cannot set up TLS: " + e.getMessage(), e);
        }
        ConnectUrl url = opts.url();
        SSLSocket s = (SSLSocket) ctx.getSocketFactory().createSocket(tcp, url.host(), url.port(), true);
        SSLParameters params = s.getSSLParameters();
        if (!opts.tlsInsecure()) {
            params.setEndpointIdentificationAlgorithm("HTTPS");
        }
        s.setSSLParameters(params);
        s.setSoTimeout(timeoutMillis);
        try {
            s.startHandshake();
        } catch (IOException e) {
            try {
                s.close();
            } catch (IOException ignored) {
                // Already failing.
            }
            throw new FireMQException("TLS handshake with " + url.authority() + " failed: " + e.getMessage(), e);
        }
        return s;
    }

    private static SSLContext context(ConnectOptions opts) throws GeneralSecurityException, IOException {
        TrustManager[] trust = null;
        if (opts.tlsInsecure()) {
            trust = new TrustManager[] {new AcceptAny()};
        } else if (opts.tlsCaPem() != null) {
            trust = withExtraCa(opts.tlsCaPem());
        }
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, trust, null);
        return ctx;
    }

    /** Trust managers that accept the platform's CAs plus the given PEM certificates. */
    private static TrustManager[] withExtraCa(byte[] pem) throws GeneralSecurityException, IOException {
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, null);
        TrustManagerFactory defaults = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        defaults.init((KeyStore) null);
        int n = 0;
        for (TrustManager tm : defaults.getTrustManagers()) {
            if (tm instanceof X509TrustManager) {
                for (X509Certificate c : ((X509TrustManager) tm).getAcceptedIssuers()) {
                    ks.setCertificateEntry("default-" + n++, c);
                }
            }
        }
        Collection<? extends Certificate> certs =
                CertificateFactory.getInstance("X.509").generateCertificates(new ByteArrayInputStream(pem));
        if (certs.isEmpty()) {
            throw new FireMQException("invalid CA certificate: no certificate found in the PEM data");
        }
        for (Certificate c : certs) {
            ks.setCertificateEntry("extra-" + n++, c);
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        return tmf.getTrustManagers();
    }

    /** Accepts any certificate. Only for {@link ConnectOptions#tlsInsecure(boolean)}. */
    private static final class AcceptAny extends X509ExtendedTrustManager {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {}

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {}

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) {}

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) {}

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {}

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {}

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
