package com.alicloud.openservices.tablestore.ecosystem;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.search.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TableCatalog implements ICatalog {
    private static final Logger LOG = LoggerFactory.getLogger(TableCatalog.class);

    /**
     * table type for catalog
     */
    public enum TableType {
        /**
         * normal kv table
         */
        Normal,

        /**
         * search index
         */
        SearchIndex
    }

    private String name;
    private TableMeta tableMeta;
    private List<TableMeta> indexMetaList = new ArrayList<TableMeta>();
    private List<IndexSchema> searchSchema = new ArrayList<IndexSchema>();

    private List<String> searchNames = new ArrayList<String>();

    public TableCatalog(String tableName) {
        name = tableName;
    }

    public String getName() {
        return name;
    }

    public  TableMeta getTableMeta() {
        return this.tableMeta;
    }

    public List<TableMeta> getIndexMetaList() {
        return this.indexMetaList;
    }

    public List<IndexSchema> getSearchSchema() {
        return this.searchSchema;
    }

    public List<String> getSearchNames() {
        return this.searchNames;
    }

    public void buildCatalog(SyncClient client) {
        buildTableAndIndexMeta(client);
        try {
            buildSearchIndexMeta(client);
        } catch (TableStoreException ex) {
            LOG.error("hit Tablestore exception during fetch search meta : {}", ex.toString());
            // swallow search ex due to searchindex may not support in some region
        }
    }

    private void buildTableAndIndexMeta(SyncClient client) {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest(
                name);
        DescribeTableResponse result = client.describeTable(describeTableRequest);
        this.tableMeta = result.getTableMeta();

        // todo add convert indexmeta to tablemeta func to reduce describe table
        for (IndexMeta meta : result.getIndexMeta()) {
            DescribeTableRequest describeTableRequest2 = new DescribeTableRequest(
                    meta.getIndexName());
            DescribeTableResponse result2 = client.describeTable(describeTableRequest2);
            this.indexMetaList.add(result2.getTableMeta());
        }
    }

    private void buildSearchIndexMeta(SyncClient client) {
        ListSearchIndexRequest request = new ListSearchIndexRequest();
        request.setTableName(name);
        List<SearchIndexInfo> indexInfos = client.listSearchIndex(request).getIndexInfos();

        for (SearchIndexInfo indexInfo : indexInfos) {
            DescribeSearchIndexRequest request2 = new DescribeSearchIndexRequest();
            request2.setTableName(indexInfo.getTableName());
            request2.setIndexName(indexInfo.getIndexName());
            DescribeSearchIndexResponse response = client.describeSearchIndex(request2);
            searchSchema.add(response.getSchema());
            searchNames.add(indexInfo.getIndexName());
        }
    }
}
