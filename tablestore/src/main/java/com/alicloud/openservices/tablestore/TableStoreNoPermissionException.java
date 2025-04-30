package com.alicloud.openservices.tablestore;

import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;

public class TableStoreNoPermissionException extends TableStoreException {

    private AccessDeniedDetail accessDeniedDetail;

    public TableStoreNoPermissionException(String message, Throwable cause,
                                           String errorCode,
                                           String requestId, int httpStatus, OtsInternalApi.AccessDeniedDetail accessDeniedDetail) {
        super(message, cause, errorCode, requestId, httpStatus);
        this.accessDeniedDetail = new AccessDeniedDetail(
                accessDeniedDetail.getAuthAction(),
                accessDeniedDetail.getAuthPrincipalType(),
                accessDeniedDetail.getAuthPrincipalOwnerId(),
                accessDeniedDetail.getAuthPrincipalDisplayName(),
                accessDeniedDetail.getPolicyType(),
                accessDeniedDetail.getNoPermissionType(),
                accessDeniedDetail.getEncodedDiagnosticMessage());
    }

    public TableStoreNoPermissionException(String message, Throwable cause,
                                           String errorCode,
                                           String requestId, int httpStatus, AccessDeniedDetail accessDeniedDetail) {
        super(message, cause, errorCode, requestId, httpStatus);
        this.accessDeniedDetail = accessDeniedDetail;
    }

    public AccessDeniedDetail getAccessDeniedDetail() {
        return accessDeniedDetail;
    }

    public void setAccessDeniedDetail(AccessDeniedDetail accessDeniedDetail) {
        this.accessDeniedDetail = accessDeniedDetail;
    }

    public String toString() {
        return super.toString() + ", [AccessDeniedDetail]:" + accessDeniedDetail.toString();
    }

    public static class AccessDeniedDetail {

        private String authAction;

        private String authPrincipalType;

        private String authPrincipalOwnerId;

        private String authPrincipalDisplayName;

        private String policyType;

        private String noPermissionType;

        private String encodedDiagnosticMessage;

        public AccessDeniedDetail(String authAction, String authPrincipalType, String authPrincipalOwnerId, String authPrincipalDisplayName, String policyType, String noPermissionType, String encodedDiagnosticMessage) {
            this.authAction = authAction;
            this.authPrincipalType = authPrincipalType;
            this.authPrincipalOwnerId = authPrincipalOwnerId;
            this.authPrincipalDisplayName = authPrincipalDisplayName;
            this.policyType = policyType;
            this.noPermissionType = noPermissionType;
            this.encodedDiagnosticMessage = encodedDiagnosticMessage;
        }

        public String getAuthAction() {
            return authAction;
        }

        public void setAuthAction(String authAction) {
            this.authAction = authAction;
        }

        public String getAuthPrincipalType() {
            return authPrincipalType;
        }

        public void setAuthPrincipalType(String authPrincipalType) {
            this.authPrincipalType = authPrincipalType;
        }

        public String getAuthPrincipalOwnerId() {
            return authPrincipalOwnerId;
        }

        public void setAuthPrincipalOwnerId(String authPrincipalOwnerId) {
            this.authPrincipalOwnerId = authPrincipalOwnerId;
        }

        public String getAuthPrincipalDisplayName() {
            return authPrincipalDisplayName;
        }

        public void setAuthPrincipalDisplayName(String authPrincipalDisplayName) {
            this.authPrincipalDisplayName = authPrincipalDisplayName;
        }

        public String getPolicyType() {
            return policyType;
        }

        public void setPolicyType(String policyType) {
            this.policyType = policyType;
        }

        public String getNoPermissionType() {
            return noPermissionType;
        }

        public void setNoPermissionType(String noPermissionType) {
            this.noPermissionType = noPermissionType;
        }

        public String getEncodedDiagnosticMessage() {
            return encodedDiagnosticMessage;
        }

        public void setEncodedDiagnosticMessage(String encodedDiagnosticMessage) {
            this.encodedDiagnosticMessage = encodedDiagnosticMessage;
        }

        @Override
        public String toString() {
            return "AccessDeniedDetail{" +
                    "authAction='" + authAction + '\'' +
                    ", authPrincipalType='" + authPrincipalType + '\'' +
                    ", authPrincipalOwnerId='" + authPrincipalOwnerId + '\'' +
                    ", authPrincipalDisplayName='" + authPrincipalDisplayName + '\'' +
                    ", policyType='" + policyType + '\'' +
                    ", noPermissionType='" + noPermissionType + '\'' +
                    ", encodedDiagnosticMessage='" + encodedDiagnosticMessage + '\'' +
                    '}';
        }
    }
}
