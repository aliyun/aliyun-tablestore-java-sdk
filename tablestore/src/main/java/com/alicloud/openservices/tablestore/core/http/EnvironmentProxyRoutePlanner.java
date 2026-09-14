package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.ClientException;
import com.aliyun.ots.thirdparty.org.apache.http.HttpHost;
import com.aliyun.ots.thirdparty.org.apache.http.HttpRequest;
import com.aliyun.ots.thirdparty.org.apache.http.impl.conn.DefaultRoutePlanner;
import com.aliyun.ots.thirdparty.org.apache.http.protocol.HttpContext;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.Locale;
import java.util.Map;

final class EnvironmentProxyRoutePlanner extends DefaultRoutePlanner {
    private final ProxySetting httpProxy;
    private final ProxySetting httpsProxy;
    private final String noProxy;

    EnvironmentProxyRoutePlanner(Map<String, String> environment) {
        super(null);
        this.httpProxy = parseProxy(firstNonEmpty(environment, "HTTP_PROXY", "http_proxy"));
        this.httpsProxy = parseProxy(firstNonEmpty(environment, "HTTPS_PROXY", "https_proxy"));
        this.noProxy = firstNonEmpty(environment, "NO_PROXY", "no_proxy");
    }

    boolean hasProxy() {
        return httpProxy != null || httpsProxy != null;
    }

    ProxySetting getHttpProxy() {
        return httpProxy;
    }

    ProxySetting getHttpsProxy() {
        return httpsProxy;
    }

    @Override
    protected HttpHost determineProxy(HttpHost target, HttpRequest request, HttpContext context) {
        ProxySetting proxy = "https".equalsIgnoreCase(target.getSchemeName()) ? httpsProxy : httpProxy;
        if (proxy == null || isBypassed(target)) {
            return null;
        }
        return proxy.host;
    }

    private boolean isBypassed(HttpHost target) {
        String targetHost = normalizeHost(target.getHostName());
        if ("localhost".equals(targetHost) || "::1".equals(targetHost) || targetHost.startsWith("127.")) {
            return true;
        }
        if (noProxy == null) {
            return false;
        }
        int targetPort = target.getPort();
        if (targetPort < 0) {
            targetPort = "https".equalsIgnoreCase(target.getSchemeName()) ? 443 : 80;
        }

        for (String value : noProxy.split(",")) {
            String entry = value.trim().toLowerCase(Locale.ROOT);
            if (entry.isEmpty()) {
                continue;
            }
            if ("*".equals(entry)) {
                return true;
            }

            int port = -1;
            String host = entry;
            if (entry.startsWith("[")) {
                int closingBracket = entry.indexOf(']');
                if (closingBracket >= 0) {
                    host = entry.substring(1, closingBracket);
                    if (closingBracket + 1 < entry.length() && entry.charAt(closingBracket + 1) == ':') {
                        port = parsePort(entry.substring(closingBracket + 2));
                    }
                }
            } else {
                int colon = entry.lastIndexOf(':');
                if (colon > 0 && entry.indexOf(':') == colon) {
                    int parsedPort = parsePort(entry.substring(colon + 1));
                    if (parsedPort >= 0) {
                        host = entry.substring(0, colon);
                        port = parsedPort;
                    }
                }
            }

            if (host.startsWith("*.")) {
                host = host.substring(2);
            }
            host = normalizeHost(host);
            if ((port < 0 || port == targetPort)
                    && (targetHost.equals(host) || targetHost.endsWith("." + host))) {
                return true;
            }
        }
        return false;
    }

    private static String firstNonEmpty(Map<String, String> environment, String upper, String lower) {
        String value = environment.get(upper);
        if (value == null || value.trim().isEmpty()) {
            value = environment.get(lower);
        }
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }

    private static ProxySetting parseProxy(String value) {
        if (value == null) {
            return null;
        }
        String proxyUrl = value.contains("://") ? value : "http://" + value;
        try {
            URI uri = new URI(proxyUrl);
            String host = uri.getHost();
            if (host == null) {
                throw new ClientException("The proxy URL is invalid: " + value);
            }
            String scheme = uri.getScheme();
            if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
                throw new ClientException("The proxy URL scheme must be http or https: " + value);
            }
            int port = uri.getPort();
            if (port < 0) {
                port = "https".equalsIgnoreCase(scheme) ? 443 : 80;
            }

            String username = null;
            String password = null;
            if (uri.getRawUserInfo() != null) {
                String[] userInfo = uri.getRawUserInfo().split(":", 2);
                username = decode(userInfo[0]);
                password = userInfo.length == 2 ? decode(userInfo[1]) : "";
            }
            return new ProxySetting(new HttpHost(host, port, scheme), username, password);
        } catch (URISyntaxException e) {
            throw new ClientException("The proxy URL is invalid: " + value, e);
        }
    }

    private static String decode(String value) {
        try {
            return URLDecoder.decode(value.replace("+", "%2B"), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String normalizeHost(String host) {
        String value = host.toLowerCase(Locale.ROOT);
        while (value.startsWith(".")) {
            value = value.substring(1);
        }
        return value;
    }

    private static int parsePort(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    static final class ProxySetting {
        private final HttpHost host;
        private final String username;
        private final String password;

        ProxySetting(HttpHost host, String username, String password) {
            this.host = host;
            this.username = username;
            this.password = password;
        }

        HttpHost getHost() {
            return host;
        }

        String getUsername() {
            return username;
        }

        String getPassword() {
            return password;
        }
    }
}
