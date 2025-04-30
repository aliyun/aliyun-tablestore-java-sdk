package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreNoPermissionException;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TablestoreNoPermissionExceptionTest {

    @Test
    public void testNoPermissionException() throws ExecutionException, InterruptedException {
        OtsInternalApi.AccessDeniedDetail accessDeniedDetail = OtsInternalApi.AccessDeniedDetail.newBuilder()
                .setAuthAction("action")
                .setAuthPrincipalOwnerId("ownerId")
                .setNoPermissionType("type")
                .setAuthPrincipalType("principal")
                .setAuthPrincipalDisplayName("displayName")
                .setPolicyType("policyType")
                .build();


        TableStoreNoPermissionException tableStoreNoPermissionException = new TableStoreNoPermissionException("message",new ClientException(),"code","1-2-3",403,accessDeniedDetail);
        assertEquals(tableStoreNoPermissionException.getAccessDeniedDetail().getAuthAction(),"action");
        assertEquals(tableStoreNoPermissionException.getAccessDeniedDetail().getAuthPrincipalOwnerId(),"ownerId");
        assertEquals(tableStoreNoPermissionException.getAccessDeniedDetail().getNoPermissionType(),"type");
        assertEquals(tableStoreNoPermissionException.getAccessDeniedDetail().getAuthPrincipalType(),"principal");
        assertEquals(tableStoreNoPermissionException.getAccessDeniedDetail().getAuthPrincipalDisplayName(),"displayName");
        assertEquals(tableStoreNoPermissionException.getAccessDeniedDetail().getPolicyType(),"policyType");
        assertEquals(tableStoreNoPermissionException.getAccessDeniedDetail().getEncodedDiagnosticMessage(),"");

        TableStoreNoPermissionException.AccessDeniedDetail accessDeniedDetail2 = tableStoreNoPermissionException.getAccessDeniedDetail();
        accessDeniedDetail2.setAuthAction("action1");
        accessDeniedDetail2.setAuthPrincipalOwnerId("ownerId1");
        accessDeniedDetail2.setNoPermissionType("type1");
        accessDeniedDetail2.setAuthPrincipalType("principal1");
        accessDeniedDetail2.setAuthPrincipalDisplayName("displayName1");
        accessDeniedDetail2.setPolicyType("policyType1");
        accessDeniedDetail2.setEncodedDiagnosticMessage("EncodedDiagnosticMessage");

        tableStoreNoPermissionException.setAccessDeniedDetail(accessDeniedDetail2);

        CallbackImpledFuture<String,String> future = new CallbackImpledFuture<>();
        future.onFailed("",tableStoreNoPermissionException);
        boolean hasTableStoreNoPermissionException = false;
        TableStoreNoPermissionException catchException = null;
        try {
            future.get();
        } catch (TableStoreNoPermissionException e) {
            hasTableStoreNoPermissionException = true;
            catchException = e;
        }
        assertTrue(hasTableStoreNoPermissionException);
        assertEquals(catchException.getMessage(),"message");
        assertEquals(catchException.getErrorCode(),"code");
        assertEquals(catchException.getAccessDeniedDetail().getAuthAction(),"action1");
        assertEquals(catchException.getAccessDeniedDetail().getAuthPrincipalOwnerId(),"ownerId1");
        assertEquals(catchException.getAccessDeniedDetail().getNoPermissionType(),"type1");
        assertEquals(catchException.getAccessDeniedDetail().getAuthPrincipalType(),"principal1");
        assertEquals(catchException.getAccessDeniedDetail().getAuthPrincipalDisplayName(),"displayName1");
        assertEquals(catchException.getAccessDeniedDetail().getPolicyType(),"policyType1");
        assertEquals(catchException.getAccessDeniedDetail().getEncodedDiagnosticMessage(),"EncodedDiagnosticMessage");
        assertTrue(catchException.getCause() instanceof TableStoreNoPermissionException);
        assertTrue(catchException.getCause().getCause() instanceof ClientException);
        assertTrue(catchException.toString().contains(", [AccessDeniedDetail]:"));

    }
}
