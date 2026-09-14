package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.aliyun.ots.thirdparty.org.apache.http.HttpHost;
import com.aliyun.ots.thirdparty.org.apache.http.message.BasicHttpRequest;
import com.aliyun.ots.thirdparty.org.apache.http.conn.routing.HttpRoute;
import com.aliyun.ots.thirdparty.org.apache.http.protocol.BasicHttpContext;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class EnvironmentProxyRoutePlannerTest {
    @Test
    public void testSelectsProxyByTargetScheme() throws Exception {
        Map<String, String> environment = new HashMap<String, String>();
        environment.put("HTTP_PROXY", "http://http-proxy.example.com:8080");
        environment.put("HTTPS_PROXY", "https://https-proxy.example.com:8443");
        EnvironmentProxyRoutePlanner planner = new EnvironmentProxyRoutePlanner(environment);

        assertEquals(
                new HttpHost("http-proxy.example.com", 8080, "http"),
                route(planner, "http", "example.com", -1).getProxyHost());
        assertEquals(
                new HttpHost("https-proxy.example.com", 8443, "https"),
                route(planner, "https", "example.com", -1).getProxyHost());
    }

    @Test
    public void testUppercaseEnvironmentTakesPrecedence() throws Exception {
        Map<String, String> environment = new HashMap<String, String>();
        environment.put("HTTP_PROXY", "upper.example.com:8080");
        environment.put("http_proxy", "lower.example.com:8081");
        EnvironmentProxyRoutePlanner planner = new EnvironmentProxyRoutePlanner(environment);

        assertEquals("upper.example.com", route(planner, "http", "example.com", -1)
                .getProxyHost().getHostName());
    }

    @Test
    public void testNoProxyDomainAndPort() throws Exception {
        Map<String, String> environment = new HashMap<String, String>();
        environment.put("HTTPS_PROXY", "proxy.example.com:8080");
        environment.put("NO_PROXY", ".aliyuncs.com,example.com:8443");
        EnvironmentProxyRoutePlanner planner = new EnvironmentProxyRoutePlanner(environment);

        assertNull(route(planner, "https", "ots.aliyuncs.com", -1).getProxyHost());
        assertNull(route(planner, "https", "example.com", 8443).getProxyHost());
        assertEquals("proxy.example.com", route(planner, "https", "example.com", 443)
                .getProxyHost().getHostName());
    }

    @Test
    public void testNoProxyWildcard() throws Exception {
        Map<String, String> environment = new HashMap<String, String>();
        environment.put("HTTP_PROXY", "proxy.example.com:8080");
        environment.put("no_proxy", "*");
        EnvironmentProxyRoutePlanner planner = new EnvironmentProxyRoutePlanner(environment);

        assertNull(route(planner, "http", "example.com", -1).getProxyHost());
    }

    @Test
    public void testLoopbackAlwaysBypassesProxy() throws Exception {
        Map<String, String> environment = new HashMap<String, String>();
        environment.put("HTTP_PROXY", "proxy.example.com:8080");
        EnvironmentProxyRoutePlanner planner = new EnvironmentProxyRoutePlanner(environment);

        assertNull(route(planner, "http", "localhost", -1).getProxyHost());
        assertNull(route(planner, "http", "127.0.0.1", -1).getProxyHost());
    }

    @Test
    public void testProxyAuthenticationAndDefaultPort() {
        Map<String, String> environment = new HashMap<String, String>();
        environment.put("HTTPS_PROXY", "https://user%40example.com:p%40ss@proxy.example.com");
        EnvironmentProxyRoutePlanner planner = new EnvironmentProxyRoutePlanner(environment);
        EnvironmentProxyRoutePlanner.ProxySetting proxy = planner.getHttpsProxy();

        assertEquals(443, proxy.getHost().getPort());
        assertEquals("user@example.com", proxy.getUsername());
        assertEquals("p@ss", proxy.getPassword());
    }

    @Test
    public void testNoEnvironmentProxy() {
        EnvironmentProxyRoutePlanner planner =
                new EnvironmentProxyRoutePlanner(new HashMap<String, String>());

        assertFalse(planner.hasProxy());
        assertNull(planner.getHttpProxy());
        assertNull(planner.getHttpsProxy());
    }

    @Test(expected = ClientException.class)
    public void testInvalidProxyUrl() {
        Map<String, String> environment = new HashMap<String, String>();
        environment.put("HTTP_PROXY", "http://bad host:8080");
        new EnvironmentProxyRoutePlanner(environment);
    }

    @Test
    public void testHasProxy() {
        Map<String, String> environment = new HashMap<String, String>();
        environment.put("http_proxy", "proxy.example.com:8080");

        assertTrue(new EnvironmentProxyRoutePlanner(environment).hasProxy());
    }

    @Test
    public void testExplicitProxyIgnoresEnvironment() {
        ClientConfiguration config = new ClientConfiguration();
        config.setProxyHost("configured-proxy.example.com");
        config.setProxyPort(8080);
        Map<String, String> environment = new HashMap<String, String>();
        environment.put("HTTP_PROXY", "http://bad host:8080");

        assertNull(HttpFactory.createEnvironmentProxyRoutePlanner(config, environment));
    }

    private static HttpRoute route(
            EnvironmentProxyRoutePlanner planner, String scheme, String host, int port) throws Exception {
        return planner.determineRoute(
                new HttpHost(host, port, scheme), new BasicHttpRequest("GET", "/"), new BasicHttpContext());
    }
}
