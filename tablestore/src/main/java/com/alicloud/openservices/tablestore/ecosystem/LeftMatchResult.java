package com.alicloud.openservices.tablestore.ecosystem;

import com.alicloud.openservices.tablestore.model.TableMeta;

import java.util.ArrayList;
import java.util.List;

/**
 * LeftMatchResult for global index selection
 */
public class LeftMatchResult{
    private List<String> leftMatchKeyList;

    public void setCanAppendNewKey(boolean canAppendNewKey) {
        this.canAppendNewKey = canAppendNewKey;
    }

    private boolean canAppendNewKey;
    private int beginPkIndex;
    private String tableName;
    private TableMeta meta;
    private boolean matchedAll;

    public List<String> getLeftMatchKeyList() {
        return leftMatchKeyList;
    }

    public int getCount() {
        return leftMatchKeyList.size();
    }

    public boolean canMatchedAll() {
        return getCount() == meta.getPrimaryKeyList().size();
    }

    public void append(List<String> keyList){
        leftMatchKeyList.addAll(keyList);
    }

    public String getTableName() {
        return tableName;
    }

    public boolean getCanAppendNewKey() {
        return canAppendNewKey;
    }

    public TableMeta getTableMeta() {
        return this.meta;
    }

    public int getBeginPkIndex() {
        return this.beginPkIndex;
    }

    public LeftMatchResult(String tableName, List<String> keyList, boolean canAppend, int beginIndex, TableMeta meta) {
        this.tableName = tableName;
        this.leftMatchKeyList = keyList;
        this.canAppendNewKey = canAppend;
        this.beginPkIndex = beginIndex;
        this.meta = meta;
    }

    public LeftMatchResult(String indexName, TableMeta meta) {
        this.canAppendNewKey = true;
        this.beginPkIndex = 0;
        this.tableName = indexName;
        this.meta = meta;
        this.leftMatchKeyList = new ArrayList<String>();
    }
    public LeftMatchResult() {
        this.leftMatchKeyList = new ArrayList<String>();
        this.canAppendNewKey = false;
    }

    public void copyTo(LeftMatchResult target) {
        target.leftMatchKeyList.addAll(this.leftMatchKeyList);
        target.tableName = this.tableName;
        target.canAppendNewKey = this.canAppendNewKey;
        target.beginPkIndex = this.beginPkIndex;
        target.meta = this.meta;
    }
}