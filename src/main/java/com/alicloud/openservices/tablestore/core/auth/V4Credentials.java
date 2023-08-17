package com.alicloud.openservices.tablestore.core.auth;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.StringUtils;

import static com.alicloud.openservices.tablestore.core.Constants.PRODUCT;
import static com.alicloud.openservices.tablestore.core.Constants.SIGNING_KEY_SIGN_METHOD;

public class V4Credentials implements ServiceCredentialsV4 {

    private static final ThreadLocal<DateFormat> DATA_FORMAT = new ThreadLocal<DateFormat>() {
        protected DateFormat initialValue() {
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
            format.setTimeZone(TimeZone.getTimeZone("UTC"));
            return format;
        }
    };

    private final String accessKeyId;
    private final String v4SigningStsToken;
    private final String region;
    private String accessKeySecret;
    private String v4SigningAccessKey;
    private String signingDate;
    private boolean autoUpdateV4SigningAccessKey = false;

    public V4Credentials(String accessKeyId, String v4SigningAccessKey, String region, String signingDate) {
        this(accessKeyId, v4SigningAccessKey, null, region, signingDate);
    }

    public V4Credentials(String accessKeyId, String v4SigningAccessKey, String v4SigningStsToken, String region, String signingDate) {
        Preconditions.checkArgument(AuthUtils.checkAccessKeyIdFormat(accessKeyId), "The access key id is not in valid format: " + accessKeyId);

        if (StringUtils.isNullOrEmpty(accessKeyId)) {
            throw new InvalidCredentialsException("Access key id should not be null or empty.");
        }
        if (StringUtils.isNullOrEmpty(v4SigningAccessKey)) {
            throw new InvalidCredentialsException("Secret access key should not be null or empty.");
        }
        if (StringUtils.isNullOrEmpty(region)) {
            throw new InvalidCredentialsException("Region for v4 signing key should not be null or empty.");
        }
        if (StringUtils.isNullOrEmpty(signingDate)) {
            throw new InvalidCredentialsException("SigningDate for v4 signing key should not be null or empty.");
        }

        this.accessKeyId = accessKeyId;
        this.v4SigningAccessKey = v4SigningAccessKey;
        this.v4SigningStsToken = v4SigningStsToken;
        this.region = region;
        this.signingDate = signingDate;
    }

    public static V4Credentials createByServiceCredentials(ServiceCredentials serviceCredentials, String region) {
        String signDate = DATA_FORMAT.get().format(new Date());
        String v4SigningAccessKey =
                CalculateV4SigningKeyUtil.finalSigningKeyString(serviceCredentials.getAccessKeySecret(), signDate, region, PRODUCT, SIGNING_KEY_SIGN_METHOD);

        V4Credentials v4Credentials =
                new V4Credentials(serviceCredentials.getAccessKeyId(), v4SigningAccessKey, serviceCredentials.getSecurityToken(), region, signDate);
        v4Credentials.accessKeySecret = serviceCredentials.getAccessKeySecret();
        v4Credentials.autoUpdateV4SigningAccessKey = true;
        return v4Credentials;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getAccessKeySecret() {
        updateV4Signature();
        return v4SigningAccessKey;
    }

    public String getSecurityToken() {
        return v4SigningStsToken;
    }

    public String getRegion() {
        return region;
    }

    public String getSigningDate() {
        return signingDate;
    }

    /**
     * 只有通过 createByServiceCredentials() 方法创建，才会自动更新v4SigningAccessKey字段
     */
    private void updateV4Signature() {
        if (autoUpdateV4SigningAccessKey) {
            String dataNow = DATA_FORMAT.get().format(new Date());
            if (!dataNow.equals(signingDate)) {
                signingDate = dataNow;
                v4SigningAccessKey = CalculateV4SigningKeyUtil.finalSigningKeyString(accessKeySecret, signingDate, region, PRODUCT, SIGNING_KEY_SIGN_METHOD);
            }
        }
    }
}