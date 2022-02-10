package com.alicloud.openservices.tablestore.model;


/**
 * <p></p>Get the partition information of the data.</p>
 * <p>Note: Later versions will incorporate {@link ComputeSplitsBySizeRequest} into this request.</p>
 */
public class ComputeSplitsRequest  implements Request {

    /**
     * the name of your table.
     */
    private String tableName;

    /**
     * <p>{@link SplitsOptions} Interface.</p>
     * <p>With {@link SearchIndexSplitsOptions}, {@link ComputeSplitsResponse} can return the maximum parallel for scan data of this index.</p>
     */
    private SplitsOptions splitsOptions;

    public ComputeSplitsRequest() {
    }

    public ComputeSplitsRequest(String tableName, SplitsOptions splitsOptions) {
        this.tableName = tableName;
        this.splitsOptions = splitsOptions;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_COMPUTE_SPLITS;
    }

    public String getTableName() {
        return tableName;
    }

    public ComputeSplitsRequest setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public SearchIndexSplitsOptions getSearchIndexSplitsOptions() {
        if (splitsOptions instanceof SearchIndexSplitsOptions) {
            return (SearchIndexSplitsOptions)splitsOptions;
        }
        return null;
    }

    public SplitsOptions getSplitsOptions() {
        return splitsOptions;
    }

    public ComputeSplitsRequest setSplitsOptions(SplitsOptions splitsOptions) {
        this.splitsOptions = splitsOptions;
        return this;
    }

    private ComputeSplitsRequest(Builder builder) {
        setTableName(builder.tableName);
        setSplitsOptions(builder.splitsOptions);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String tableName;
        private SplitsOptions splitsOptions;

        private Builder() {}

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder splitsOptions(SplitsOptions searchIndexSplitsOptions) {
            this.splitsOptions = searchIndexSplitsOptions;
            return this;
        }

        public ComputeSplitsRequest build() {
            return new ComputeSplitsRequest(this);
        }
    }
}
