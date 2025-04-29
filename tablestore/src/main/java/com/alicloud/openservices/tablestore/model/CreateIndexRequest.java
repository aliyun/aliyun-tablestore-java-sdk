package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * CreateIndexRequest contains some necessary parameters for creating a new index table, including the table's Meta.
 * After initializing the instance, you can change the Meta of the index table by calling {@link #setIndexMeta(IndexMeta)}.
 */
public class CreateIndexRequest implements Request {
    /**
     * Main table name
     */
    private String mainTableName;

    /**
     * Index table information.
     */
    private IndexMeta indexMeta;

    /*
     * Whether to include existing data in the main table
     */
    private boolean includeBaseData;

    /**
     * Initialize the CreateIndexRequest instance.
     *
     * @param mainTableName The name of the main table.
     * @param indexMeta The structural information of the index table.
     * @param includeBaseData Whether the newly created index table should include the existing data from the main table.
     */
    public CreateIndexRequest(String mainTableName, IndexMeta indexMeta, boolean includeBaseData) {
        setMainTableName(mainTableName);
        setIndexMeta(indexMeta);
        setIncludeBaseData(includeBaseData);

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
     * Get the name of the main table.
     *
     * @return the name of the main table
     */
    public String getMainTableName() {
        return mainTableName;
    }

    /**
     * Get the structure information of the index table
     *
     * @return Structure information of the index table
     */
    public IndexMeta getIndexMeta() {
        return indexMeta;
    }

    /**
     * Check if the index table contains existing data from the main table when creating an indexed table.
     * @return Whether it includes existing data from the main table.
     */
    public boolean getIncludeBaseData() { return includeBaseData; }

    @Override
    public String getOperationName() {
        return OperationNames.OP_CREATE_INDEX;
    }

}
