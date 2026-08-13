package com.alicloud.openservices.tablestore;

import com.alicloud.openservices.tablestore.model.memory.*;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MemoryClientApiTest {
    private static final Api[] APIS = {
            api("createMemoryStore", CreateMemoryStoreRequest.class, CreateMemoryStoreResponse.class),
            api("getMemoryStore", GetMemoryStoreRequest.class, GetMemoryStoreResponse.class),
            api("listMemoryStores", ListMemoryStoresRequest.class, ListMemoryStoresResponse.class),
            api("updateMemoryStore", UpdateMemoryStoreRequest.class, UpdateMemoryStoreResponse.class),
            api("deleteMemoryStore", DeleteMemoryStoreRequest.class, DeleteMemoryStoreResponse.class),
            api("addMemories", AddMemoriesRequest.class, AddMemoriesResponse.class),
            api("searchMemories", SearchMemoriesRequest.class, SearchMemoriesResponse.class),
            api("listMemories", ListMemoriesRequest.class, ListMemoriesResponse.class),
            api("getMemory", GetMemoryRequest.class, GetMemoryResponse.class),
            api("updateMemory", UpdateMemoryRequest.class, UpdateMemoryResponse.class),
            api("deleteMemory", DeleteMemoryRequest.class, DeleteMemoryResponse.class),
            api("listMemoryStoreMessages", ListMemoryStoreMessagesRequest.class, ListMemoryStoreMessagesResponse.class),
            api("listMemoryStoreRequests", ListMemoryStoreRequestsRequest.class, ListMemoryStoreRequestsResponse.class),
            api("getMemoryTask", GetMemoryTaskRequest.class, GetMemoryTaskResponse.class),
            api("listMemoryTasks", ListMemoryTasksRequest.class, ListMemoryTasksResponse.class),
            api("listMemoryStoreScopes", ListMemoryStoreScopesRequest.class, ListMemoryStoreScopesResponse.class),
            api("createMemoryDreamTask", CreateMemoryDreamTaskRequest.class, CreateMemoryDreamTaskResponse.class),
            api("getMemoryDreamTask", GetMemoryDreamTaskRequest.class, GetMemoryDreamTaskResponse.class),
            api("listMemoryDreamTasks", ListMemoryDreamTasksRequest.class, ListMemoryDreamTasksResponse.class),
            api("cancelMemoryDreamTask", CancelMemoryDreamTaskRequest.class, CancelMemoryDreamTaskResponse.class),
            api("listMemoryDreamActions", ListMemoryDreamActionsRequest.class, ListMemoryDreamActionsResponse.class),
            api("applyMemoryDreamActions", ApplyMemoryDreamActionsRequest.class, ApplyMemoryDreamActionsResponse.class),
            api("addItem", AddItemRequest.class, AddItemResponse.class),
            api("listItems", ListItemsRequest.class, ListItemsResponse.class),
            api("getItem", GetItemRequest.class, GetItemResponse.class),
            api("updateItem", UpdateItemRequest.class, UpdateItemResponse.class),
            api("deleteItem", DeleteItemRequest.class, DeleteItemResponse.class),
            api("listItemVersions", ListItemVersionsRequest.class, ListItemVersionsResponse.class),
            api("getItemVersion", GetItemVersionRequest.class, GetItemVersionResponse.class),
            api("redactItemVersion", RedactItemVersionRequest.class, RedactItemVersionResponse.class)
    };

    @Test
    public void exposesEveryMemoryOperationOnPublicClients() throws Exception {
        for (Api api : APIS) {
            assertSyncMethod(SyncClientInterface.class, api);
            assertSyncMethod(SyncClient.class, api);
            assertAsyncMethod(AsyncClientInterface.class, api);
            assertAsyncMethod(AsyncClient.class, api);
        }
    }

    @Test
    public void keepsThirdPartyClientImplementationsSourceCompatible() throws Exception {
        for (Api api : APIS) {
            Method syncMethod = SyncClientInterface.class.getMethod(api.methodName, api.requestClass);
            assertTrue("SyncClientInterface." + api.methodName + " must be a default method",
                    syncMethod.isDefault());

            Method asyncMethod = AsyncClientInterface.class.getMethod(
                    api.methodName, api.requestClass, TableStoreCallback.class);
            assertTrue("AsyncClientInterface." + api.methodName + " must be a default method",
                    asyncMethod.isDefault());
        }
    }

    private static void assertSyncMethod(Class<?> clientClass, Api api) throws Exception {
        Method method = clientClass.getMethod(api.methodName, api.requestClass);
        assertEquals(clientClass.getSimpleName() + "." + api.methodName, api.responseClass, method.getReturnType());
    }

    private static void assertAsyncMethod(Class<?> clientClass, Api api) throws Exception {
        Method method = clientClass.getMethod(api.methodName, api.requestClass, TableStoreCallback.class);
        assertEquals(clientClass.getSimpleName() + "." + api.methodName, Future.class, method.getReturnType());
        ParameterizedType returnType = (ParameterizedType) method.getGenericReturnType();
        assertEquals(clientClass.getSimpleName() + "." + api.methodName,
                api.responseClass, returnType.getActualTypeArguments()[0]);
    }

    private static Api api(String methodName, Class<?> requestClass, Class<?> responseClass) {
        return new Api(methodName, requestClass, responseClass);
    }

    private static class Api {
        private final String methodName;
        private final Class<?> requestClass;
        private final Class<?> responseClass;

        private Api(String methodName, Class<?> requestClass, Class<?> responseClass) {
            this.methodName = methodName;
            this.requestClass = requestClass;
            this.responseClass = responseClass;
        }
    }
}
