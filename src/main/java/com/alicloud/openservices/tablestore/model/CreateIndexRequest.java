package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * CreateIndexRequest包含创建一张新的索引表所必需的一些参数，包括表的Meta
 * 在初始化实例之后，可以通过调用{@link #setIndexMeta(IndexMeta)} 来更改索引表的Meta。

 */
public class CreateIndexRequest implements Request {
    /**
     * 主表名
     */
    private String mainTableName;

    /**
     * 索引表信息。
     */
    private IndexMeta indexMeta;

    /*
     * 是否包含主表中已有数据
     */
    private boolean includeBaseData;

    /**
     * 初始化CreateIndexRequest实例。
     *
     * @param mainTableName 主表的名字。
     * @param indexMeta 索引表的结构信息。
     * @param includeBaseData 新建索引表是否要包含主表中已有数据
     */
    public CreateIndexRequest(String mainTableName, IndexMeta indexMeta, boolean includeBaseData) {
        setMainTableName(mainTableName);
        setIndexMeta(indexMeta);

    }

    public void setMainTableName(String mainTableName) {
        Preconditions.checkArgument(mainTableName != null && !mainTableName.isEmpty(),
                "The main table name should not be null or empty");;
        this.mainTableName = mainTableName;
    }

    public void setIndexMeta(IndexMeta indexMeta) {
        Preconditions.checkArgument(indexMeta != null, "The index meta should not be null");
        this.indexMeta = indexMeta;
    }

    public void setIncludeBaseData(boolean includeBaseData) {
        this.includeBaseData = includeBaseData;
    }
    /**
     * 获取主表的名字。
     *
     * @return 主表的名字
     */
    public String getMainTableName() {
        return mainTableName;
    }

    /**
     * 获取索引表的结构信息
     *
     * @return 索引表的结构信息
     */
    public IndexMeta getIndexMeta() {
        return indexMeta;
    }

    /**
     * 获取创建索引索引表时，索引表中是否包含主表已有数据
     * @return 是否包含主表已有数据
     */
    public boolean getIncludeBaseData() { return includeBaseData; }

    @Override
    public String getOperationName() {
        return OperationNames.OP_CREATE_INDEX;
    }

}
