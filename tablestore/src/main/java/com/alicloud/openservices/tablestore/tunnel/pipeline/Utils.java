package com.alicloud.openservices.tablestore.tunnel.pipeline;

import com.alicloud.openservices.tablestore.core.protocol.TunnelServiceApi;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    public static TunnelServiceApi.TokenContentV2 parseRequestToken(String token) throws Exception {
        TunnelServiceApi.Token tokenPb = TunnelServiceApi.Token.parseFrom(Base64.decodeBase64(token));
        if (!tokenPb.hasVersion()) {
            throw new Exception("token miss must field: version.");
        }

        switch ((int)tokenPb.getVersion()) {
            case 1:
                TunnelServiceApi.TokenContent tokenContent = TunnelServiceApi.TokenContent.parseFrom(tokenPb.getContent());
                TunnelServiceApi.TokenContentV2.Builder tokenBuilder = TunnelServiceApi.TokenContentV2.newBuilder();
                tokenBuilder.setPrimaryKey(tokenContent.getPrimaryKey());
                tokenBuilder.setTimestamp(tokenContent.getTimestamp());
                tokenBuilder.setIterator(tokenContent.getIterator());
                tokenBuilder.setTotalCount(0);
                return tokenBuilder.build();
            case 2:
                return TunnelServiceApi.TokenContentV2.parseFrom(tokenPb.getContent());
            default:
                throw new Exception(String.format("token version %d not support", tokenPb.getVersion()));
        }
    }

    /**
     * TODO: Get channel type directly from tunnel server.
     */
    public static boolean isStreamToken(String token) {
        try {
            TunnelServiceApi.TokenContentV2 tokenContentV2 = parseRequestToken(token);
            return !tokenContentV2.getIterator().isEmpty();
        } catch (Exception e) {
            LOG.error("parse token error, detail: {}", e.toString());
            return false;
        }
    }
}
