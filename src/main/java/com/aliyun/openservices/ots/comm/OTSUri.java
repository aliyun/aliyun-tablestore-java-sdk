package com.aliyun.openservices.ots.comm;

import org.apache.http.HttpHost;
import org.apache.http.client.utils.URIUtils;

import java.net.URI;
import java.net.URISyntaxException;

public class OTSUri {
    private URI uri;
    private HttpHost host;
    private String action;

    public OTSUri(String endpoint, String action) {
        this.action = action;

        final String delimiter = "/";
        if (!endpoint.endsWith(delimiter)) {
            endpoint += delimiter;
        }

        // keep only one '/' in the end
        int index = endpoint.length() - 1;
        while (index > 0 && endpoint.charAt(index - 1) == '/') {
            index--;
        }

        endpoint = endpoint.substring(0, index + 1);

        try {
            this.uri = new URI(endpoint + action);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("The endpoint is invalid.", e);
        }

        this.host = URIUtils.extractHost(uri);
    }

    public URI getUri() {
        return uri;
    }

    public HttpHost getHost() {
        return host;
    }

    public String getAction() {
        return action;
    }
}
