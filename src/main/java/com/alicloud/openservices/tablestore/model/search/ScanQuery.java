package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.QueryBuilder;
import com.alicloud.openservices.tablestore.model.ComputeSplitsResponse;

/**
 * Query for scan data.
 *
 * <p>Function:</p>
 * <p>The function is to scan data in batches.</p>
 *
 * <p>Usage scenarios:</p>
 * <p>When you want to scan data in bulk, and you don't care about sorting, aggregation, etc.</p>
 * <p>{@link ScanQuery} supports multiple threads to access data in parallel.</p>
 * <p>{@link ScanQuery#setLimit(Integer)} can be very large and you can san data faster.</p>
 *
 * <p>Exception:</p>
 * <p>{@link TableStoreException}:</p>
 * <p>When you scan data, there may be an exception that session is expired. {@link TableStoreException#getErrorCode()} is OTSServerSessionExpired.</p>
 * <p>For Example:
 * <ul>
 *     <li>Scan data after the time of aliveTime.</li>
 *     <li>The Tablestore service is being upgraded.</li>
 *     <li>The request has a network timeout.</li>
 * </ul>
 * <p>You should retry ComputeSplitsRequest and then retry ScanQuery.</p>
 */
public class ScanQuery{

    /**
     * Query statement, which decides what data you can get.
     */
    private Query query;

    /**
     * How many pieces of data are returned in a network request.
     */
    private Integer limit;

    /**
     * Maximum number of parallels. The default value is 1.
     * Its value can refer to {@link ComputeSplitsResponse#getSplitsSize()}. It is not allowed to exceed the reference value.
     * <p>{@link ScanQuery} supports multiple threads to access data in parallel.
     * Every thread uses the same {@link ScanQuery#query}, but you should set a different {@link ScanQuery#currentParallelId}</p>.
     */
    private Integer maxParallel;

    /**
     * ID of parallel, value range is [0, maxParallel)
     */
    private Integer currentParallelId;

    /**
     * The aliveTime of this request.
     * Unit: second, the default is 60 seconds. The request needs to be re initiated after timeout.
     * The aliveTime will be refreshed every time the data is fetched.
     */
    private Integer aliveTime;

    /**
     * Token used for page turning.
     * It is recommended to use the iterator interface of {@link SyncClientInterface#createParallelScanIterator(ParallelScanRequest)}.
     * It has encapsulated continuous page turning calls and does not require users to directly use this parameter.
     */
    private byte[] token;

    public ScanQuery() {
    }

    public Query getQuery() {
        return query;
    }

    public ScanQuery setQuery(Query query) {
        this.query = query;
        return this;
    }

    public Integer getLimit() {
        return limit;
    }

    public ScanQuery setLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public Integer getMaxParallel() {
        return maxParallel;
    }

    public ScanQuery setMaxParallel(Integer maxParallel) {
        this.maxParallel = maxParallel;
        return this;
    }

    public Integer getCurrentParallelId() {
        return currentParallelId;
    }

    public ScanQuery setCurrentParallelId(Integer currentParallelId) {
        this.currentParallelId = currentParallelId;
        return this;
    }

    public Integer getAliveTime() {
        return aliveTime;
    }

    public ScanQuery setAliveTime(Integer aliveTime) {
        this.aliveTime = aliveTime;
        return this;
    }

    public byte[] getToken() {
        return token;
    }

    public ScanQuery setToken(byte[] token) {
        this.token = token;
        return this;
    }

    private ScanQuery(Builder builder) {
        setQuery(builder.query);
        setLimit(builder.limit);
        setMaxParallel(builder.maxParallel);
        setCurrentParallelId(builder.currentParallelId);
        setAliveTime(builder.aliveTime);
        setToken(builder.token);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private Query query;
        private Integer limit;
        private Integer maxParallel;
        private Integer currentParallelId;
        private Integer aliveTime;
        private byte[] token;

        private Builder() {}

        public Builder query(QueryBuilder query) {
            this.query = query.build();
            return this;
        }

        public Builder query(Query query) {
            this.query = query;
            return this;
        }

        public Builder limit(int limit) {
            this.limit = limit;
            return this;
        }

        public Builder maxParallel(int maxParallel) {
            this.maxParallel = maxParallel;
            return this;
        }

        public Builder currentParallelId(int currentParallelId) {
            this.currentParallelId = currentParallelId;
            return this;
        }

        public Builder aliveTimeInSeconds(int aliveTime) {
            this.aliveTime = aliveTime;
            return this;
        }

        /**
         * Token used for page turning.
         * It is recommended to use the iterator interface of {@link SyncClientInterface#createParallelScanIterator(ParallelScanRequest)}.
         * It has encapsulated continuous page turning calls and does not require users to directly use this parameter.
         */
        public Builder token(byte[] token) {
            this.token = token;
            return this;
        }

        public ScanQuery build() {
            return new ScanQuery(this);
        }
    }
}
