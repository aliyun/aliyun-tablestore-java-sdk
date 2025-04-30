package examples;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.core.ResourceManager;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentialProvider;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentials;
import com.alicloud.openservices.tablestore.core.auth.V4Credentials;
import com.alicloud.openservices.tablestore.model.ListTableResponse;

public class CredentialsSample {

    private static String endpoint = "";
    private static String instanceName = "";
    private static String accessKeyId = "";
    private static String accessKeySecret = "";

    private static String v4SigningKey = "";
    private static String v4StsSigningKey = "";

    private static String region = "cn-shanghai";

    public static void main(String[] args) {
        {
            /**
             * Use v2 signature (original identity authentication method) example: directly use the original accessKeyId and accessKeySecret to construct {@link DefaultCredentials }
             */
            DefaultCredentials credentials = new DefaultCredentials(accessKeyId, accessKeySecret);
            CredentialsProvider provider = new DefaultCredentialProvider(credentials);

            /**
             * using {@link DefaultCredentials } initialize tableStore client
             */
            SyncClient client = new SyncClient(endpoint, provider, instanceName, null, new ResourceManager(null, null));
            // do something
            ListTableResponse response = client.listTable();
            System.out.println("request id : " + response.getRequestId());
            System.out.println("tableNames : " + response.getTableNames());
            // shutdown tableStore client
            client.shutdown();
        }

        {
            /**
             * Example one of using v4 signature: using the original accessKeyId and accessKeySecret -> first construct {@link DefaultCredentials }, then generate {@link V4Credentials }
             */
            DefaultCredentials credentials = new DefaultCredentials(accessKeyId, accessKeySecret);
            V4Credentials credentialsV4 = V4Credentials.createByServiceCredentials(credentials, region);
            CredentialsProvider provider = new DefaultCredentialProvider(credentialsV4);

            /**
             * using {@link V4Credentials } initialize tableStore client
             */
            SyncClient client = new SyncClient(endpoint, provider, instanceName, null, new ResourceManager(null, null));
            // do something
            ListTableResponse response = client.listTable();
            System.out.println("request id : " + response.getRequestId());
            System.out.println("tableNames : " + response.getTableNames());
            // shutdown tableStore client
            client.shutdown();
        }

        {
            /**
             * Example of using v4 signature II: Directly use accessKeyId and derived key -> Directly construct {@link V4Credentials }
             */
            Date date = new Date();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
            String signDate = dateFormat.format(date);      // The format of signDate is like "20230527"
            V4Credentials credentialsV4 = new V4Credentials(accessKeyId, v4SigningKey, v4StsSigningKey, region, signDate);
            CredentialsProvider provider = new DefaultCredentialProvider(credentialsV4);

            /**
             * using {@link V4Credentials } initialize tableStore client
             */
            SyncClient client = new SyncClient(endpoint, provider, instanceName, null, new ResourceManager(null, null));
            // do something
            ListTableResponse response = client.listTable();
            System.out.println("request id : " + response.getRequestId());
            System.out.println("tableNames : " + response.getTableNames());
            // shutdown tableStore client
            client.shutdown();
        }
    }
}
