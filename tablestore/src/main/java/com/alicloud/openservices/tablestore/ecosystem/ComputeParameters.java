package com.alicloud.openservices.tablestore.ecosystem;

public class ComputeParameters {
    public ComputeParameters(int maxSplitsCount, long splitSizeInMBs, ComputeMode computeMode) {
        this.maxSplitsCount = maxSplitsCount;
        this.splitSizeInMBs = splitSizeInMBs;
        this.computeMode = computeMode;
    }

    public ComputeParameters(String searchIndexName, int maxSplitsCount) {
        this.computeMode = ComputeMode.Search;
        this.searchIndexName = searchIndexName;
        this.maxSplitsCount = maxSplitsCount;
    }

    /**
     * compute mode
     */
    public enum ComputeMode {
        /**
         * auto choose secondary index and search index
         */
        Auto,

        /**
         * scan kv
         */
        KV,

        /**
         * Scan search
         */
        Search
    }

    private int maxSplitsCount;
    private long splitSizeInMBs;
    private ComputeMode computeMode;
    private String searchIndexName;

    public ComputeParameters() {
        computeMode = ComputeMode.KV;
        maxSplitsCount = 1000;
        splitSizeInMBs = 10;
    }

    public int getMaxSplitsCount() {
        return maxSplitsCount;
    }

    public long getSplitSizeInMBs() {
        return splitSizeInMBs;
    }

    public ComputeMode getComputeMode() {
        return computeMode;
    }

    public String getSearchIndexName() {
        return searchIndexName;
    }

    public void setComputeMode(ComputeMode computeMode) {
        this.computeMode = computeMode;
    }
}
