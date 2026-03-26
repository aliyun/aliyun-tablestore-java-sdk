package examples;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.knowledgebase.CreateKnowledgeBaseRequest;
import com.alicloud.openservices.tablestore.model.knowledgebase.CreateKnowledgeBaseResponse;
import com.alicloud.openservices.tablestore.model.knowledgebase.ListKnowledgeBaseRequest;
import com.alicloud.openservices.tablestore.model.knowledgebase.ListKnowledgeBaseResponse;
import com.google.gson.Gson;

public class KnowledgebaseSample {
    private static final String endpoint = "";
    private static final String accessKeyId = "";
    private static final String accessKeySecret = "";
    private static final String instanceName = "";

    public static void main(String[] args) {
        SyncClient client = new SyncClient(endpoint, accessKeyId, accessKeySecret, instanceName);

        createKnowledgeBase(client, "test_kb_" + System.currentTimeMillis());
        listKnowledgeBase(client);
    }

    private static void createKnowledgeBase(SyncClient client, String knowledgeBaseName) {
        // 创建知识库。
        CreateKnowledgeBaseRequest request = new CreateKnowledgeBaseRequest();
        request.setKnowledgeBaseName(knowledgeBaseName);
        CreateKnowledgeBaseResponse res = client.createKnowledgeBase(request);
        System.out.println(res.getCode());
    }

    private static void listKnowledgeBase(SyncClient client) {
        // 列出知识库。
        ListKnowledgeBaseRequest request = new ListKnowledgeBaseRequest();
        ListKnowledgeBaseResponse res = client.listKnowledgeBase(request);
        System.out.println(new Gson().toJson(res.getData()));
    }
}
