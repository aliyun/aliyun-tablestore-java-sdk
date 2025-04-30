package examples;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryRequest;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryResponse;
import com.alicloud.openservices.tablestore.model.sql.SQLResultSet;
import com.alicloud.openservices.tablestore.model.sql.SQLRow;
import com.alicloud.openservices.tablestore.model.sql.SQLTableMeta;
import com.alicloud.openservices.tablestore.model.sql.SQLUtils;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;

public class SQLQuerySample {

    public static void main(String args[]) {
        final String endPoint = "";
        final String accessId = "";
        final String accessKey = "";
        final String instanceName = "";

        SyncClientInterface client = new SyncClient(endPoint, accessId, accessKey, instanceName);
        try {
            // drop mapping table fb_test;
            System.out.println("drop mapping table begin");
            SQLQueryRequest dropMappingTableRequest = new SQLQueryRequest("drop mapping table fb_test");
            SQLQueryResponse dropMappingTableResponse = client.sqlQuery(dropMappingTableRequest);
            System.out.println("response type: " + dropMappingTableResponse.getSQLStatementType());
            System.out.println("drop mapping table end");

            // create table fb_test (pk varchar(1024), long_value bigint, double_value double, string_value mediumtext, bool_value bool, binary_value mediumblob, primary key(pk));
            System.out.println("create table begin");
            SQLQueryRequest createTableRequest = new SQLQueryRequest("create table fb_test (pk varchar(1024), long_value bigint, double_value double, string_value mediumtext, bool_value bool, binary_value mediumblob, primary key(pk))");
            SQLQueryResponse createTableResponse = client.sqlQuery(createTableRequest);
            System.out.println("response type: " + createTableResponse.getSQLStatementType());
            System.out.println("create table end");

            // desc fb_test
            System.out.println("desc table begin");
            SQLQueryRequest descTableRequest = new SQLQueryRequest("desc fb_test");
            SQLQueryResponse descTableResponse = client.sqlQuery(descTableRequest);
            System.out.println("response type: " + descTableResponse.getSQLStatementType());
            SQLResultSet descTableResultSet = descTableResponse.getSQLResultSet();
            SQLTableMeta descTableMeta = descTableResultSet.getSQLTableMeta();
            System.out.println(descTableMeta.getSchema());
            while (descTableResultSet.hasNext()) {
                SQLRow row = descTableResultSet.next();
                System.out.println(row.getString(0) + ", " + row.getString(1) + ", " +
                        row.getString(2) + ", " + row.getString(3) + ", " +
                        row.getString(4) + ", " + row.getString(5));
            }
            System.out.println("desc table end");

            // show index in fb_test
            System.out.println("show index begin");
            SQLQueryRequest showIndexRequest = new SQLQueryRequest("show index in fb_test");
            SQLQueryResponse showIndexResponse = client.sqlQuery(showIndexRequest);
            System.out.println("response type: " + showIndexResponse.getSQLStatementType());
            SQLResultSet showIndexResultSet = showIndexResponse.getSQLResultSet();
            SQLTableMeta showIndexTableMeta = showIndexResultSet.getSQLTableMeta();
            System.out.println(showIndexTableMeta.getSchema());
            while (showIndexResultSet.hasNext()) {
                SQLRow row = showIndexResultSet.next();
                System.out.println(row.getString("Table") + ", " + row.getLong("Non_unique") + ", " +
                        row.getString("Key_name") + ", " + row.getLong("Seq_in_index") + ", " +
                        row.getString("Column_name") + ", " + row.getString("Index_type") );
            }
            System.out.println("show index end");

            // show tables;
            System.out.println("show tables begin");
            SQLQueryRequest showTableRequest = new SQLQueryRequest("show tables");
            SQLQueryResponse showTableResponse = client.sqlQuery(showTableRequest);
            System.out.println("response type: " + showTableResponse.getSQLStatementType());
            SQLResultSet showTableResultSet = showTableResponse.getSQLResultSet();
            SQLTableMeta showTableMeta = showTableResultSet.getSQLTableMeta();
            System.out.println(showTableMeta.getSchema());
            while (showTableResultSet.hasNext()) {
                SQLRow row = showTableResultSet.next();
                System.out.println(row.getString(0));
            }
            List<String> tables = SQLUtils.parseShowTablesResponse(showTableResponse);
            for (String table : tables) {
                System.out.print(table + ", ");
            }
            System.out.println();
            System.out.println("show tables end");

            // select pk, long_value, double_value, string_value, bool_value, binary_value FROM fb_test limit 20;
            System.out.println("select query begin");
            SQLQueryRequest selectRequest = new SQLQueryRequest("select pk, long_value, double_value, string_value, bool_value, binary_value from fb_test limit 20");
            SQLQueryResponse selectResponse = client.sqlQuery(selectRequest);
            System.out.println("response type: " + selectResponse.getSQLStatementType());
            SQLTableMeta selectMeta = selectResponse.getSQLResultSet().getSQLTableMeta();
            System.out.println(selectMeta.getSchema());
            SQLResultSet selectResultSet = selectResponse.getSQLResultSet();
            while (selectResultSet.hasNext()) {
                SQLRow row = selectResultSet.next();
                System.out.println(row.toDebugString());
                System.out.println(row.getString(0) + ", " + row.getString("pk") + ", " +
                        row.getLong(1) + ", " + row.getLong("long_value") + ", " +
                        row.getDouble(2) + ", " + row.getDouble("double_value") + ", " +
                        row.getString(3) + ", " + row.getString("string_value") + ", " +
                        row.getBoolean(4) + ", " + row.getBoolean("bool_value") + ", " +
                        byteBufferToString(row.getBinary(5)) + ", " + byteBufferToString(row.getBinary("binary_value"))
                );
            }
            System.out.println("select query end");

            //select col_str_0, col_int_1, col_int_2, col_int_3, col_int_4 from test_search_agg_token group by col_str_0, col_int_1, col_int_2, col_int_3, col_int_4 order by col_str_0 limit 10000;
            long totalRowCount = 0;
            int cnt = 0;
            SQLQueryRequest searchTokenRequest = new SQLQueryRequest("select col_str_0, col_int_1, col_int_2, col_int_3, col_int_4 from test_search_agg_token group by col_str_0, col_int_1, col_int_2, col_int_3, col_int_4 order by col_str_0 limit 10000;");
            while (true) {
                SQLQueryResponse searchTokenResponse = client.sqlQuery(searchTokenRequest);
                SQLResultSet selectSearchResultSet = searchTokenResponse.getSQLResultSet();
                if (selectSearchResultSet.hasNext()) {
                    totalRowCount += selectSearchResultSet.rowCount();
                }
                cnt ++;
                System.out.println("iterator: " + cnt + ", totalRowCount: " + totalRowCount);
                if (searchTokenResponse.getNextSearchToken() != null) {
                    searchTokenRequest.setSearchToken(searchTokenResponse.getNextSearchToken());
                } else {
                    break;
                }
            }
            System.out.println("totalRowCount: " + totalRowCount);

            // timeseries query
            System.out.println("timeseries query begin");
            SQLQueryRequest timeseriesRequest = new SQLQueryRequest("select * from devops_25w limit 10");
            SQLQueryResponse timeseriesResponse = client.sqlQuery(timeseriesRequest);
            System.out.println("response type: " + timeseriesResponse.getSQLStatementType());
            SQLTableMeta timeseriesMeta = timeseriesResponse.getSQLResultSet().getSQLTableMeta();
            System.out.println(timeseriesMeta.getSchema());
            SQLResultSet timeseriesResultSet = timeseriesResponse.getSQLResultSet();
            while (timeseriesResultSet.hasNext()) {
                SQLRow row = timeseriesResultSet.next();
                System.out.println(row.toDebugString());
            }
            System.out.println("timeseries query end");

            //select datetime_value,time_value,date_value
            System.out.println("date type select query begin");
            SQLQueryRequest dateTypeSelectRequest = new SQLQueryRequest("select from_unixtime(1689705552.010),timediff(from_unixtime(1699496141.123),from_unixtime(1699496041.257)),date(from_unixtime(1689705552.010))");
            SQLQueryResponse dateTypeSelectResponse = client.sqlQuery(dateTypeSelectRequest);
            System.out.println("response type: " + dateTypeSelectResponse.getSQLStatementType());
            SQLTableMeta dateTypeSelectMeta = dateTypeSelectResponse.getSQLResultSet().getSQLTableMeta();
            System.out.println(dateTypeSelectMeta.getSchema());
            SQLResultSet dateTypeSelectResultSet = dateTypeSelectResponse.getSQLResultSet();
            while (dateTypeSelectResultSet.hasNext()) {
                SQLRow row = dateTypeSelectResultSet.next();
                System.out.println(row.toDebugString());
                System.out.println(row.getDateTime(0).withZoneSameInstant(ZoneId.systemDefault()) + ", " + row.getDateTime("from_unixtime(1689705552.010)").withZoneSameInstant(ZoneId.systemDefault()) + ", " +
                        row.getTime(1) + ", " + row.getTime("timediff(from_unixtime(1699496141.123),from_unixtime(1699496041.257))") + ", " +
                        row.getDate(2) + ", " + row.getDate("date(from_unixtime(1689705552.010))"));
            }
            System.out.println("select query end");
        } catch (TableStoreException e) {
            System.err.println("operation failed, detail: " + e.getMessage());
            // You can handle errors based on the error code. The ErrorCode of OTS is defined in OTSErrorCode.
            if (ErrorCode.QUOTA_EXHAUSTED.equals(e.getErrorCode())) {
                System.err.println("Quota exhausted");
            }
            // The request ID can be used to contact customer service to diagnose abnormalities when there are problems.
            System.err.println("Request ID:" + e.getRequestId());
        } catch (ClientException e) {
            // It might be due to poor network conditions or issues with the returned results.
            System.err.println("request failed, detail: " + e.getMessage());
        } catch (Exception e) {
            System.err.println(e);
        } finally {
            client.shutdown();
        }
    }

    private static String byteBufferToString(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        } else {
            byte[] array = new byte[byteBuffer.remaining()];
            byteBuffer.get(array);
            return new String(array);
        }
    }

}
