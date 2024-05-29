package com.alicloud.openservices.tablestore.core.protocol.timeseries;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.*;
import com.alicloud.openservices.tablestore.model.Error;
import com.alicloud.openservices.tablestore.model.TimeseriesMetaOptions;
import com.alicloud.openservices.tablestore.model.TimeseriesTableMeta;
import com.alicloud.openservices.tablestore.model.TimeseriesTableOptions;
import com.alicloud.openservices.tablestore.model.timeseries.*;
import com.google.common.cache.Cache;

import java.io.IOException;
import java.util.*;

public class TimeseriesResponseFactory {

    private static final String HASH_COLUMN_NAME = "_#h";
    private static final String MEASUREMENT_COLUMN_NAME = "_m_name";
    private static final String DATASOURCE_COLUMN_NAME = "_data_source";
    private static final String TAGS_COLUMN_NAME = "_tags";
    private static final String TIME_COLUMN_NAME = "_time";

    public static CreateTimeseriesTableResponse createCreateTimeseriesTableResponse(
            ResponseContentWithMeta response, Timeseries.CreateTimeseriesTableResponse pbResponse) {
        return new CreateTimeseriesTableResponse(response.getMeta());
    }

    public static PutTimeseriesDataResponse createPutTimeseriesDataResponse(
            ResponseContentWithMeta meta, Timeseries.PutTimeseriesDataResponse pbResponse, PutTimeseriesDataRequest request, Cache<String, Long> timeseriesMetaCache) {
        PutTimeseriesDataResponse response = new PutTimeseriesDataResponse(meta.getMeta());
        if (pbResponse.getFailedRowsCount() > 0) {
            List<PutTimeseriesDataResponse.FailedRowResult> failedRowResultList =
                    new ArrayList<PutTimeseriesDataResponse.FailedRowResult>();
            for (int i = 0; i < pbResponse.getFailedRowsCount(); i++) {
                Timeseries.FailedRowInfo failedRowInfo = pbResponse.getFailedRows(i);
                failedRowResultList.add(new PutTimeseriesDataResponse.FailedRowResult(
                        failedRowInfo.getRowIndex(),
                        new Error(failedRowInfo.getErrorCode(), failedRowInfo.getErrorMessage())));
            }
            response.setFailedRows(failedRowResultList);
        }
        if (pbResponse.hasMetaUpdateStatus()) {
            List<Integer> rowIds = pbResponse.getMetaUpdateStatus().getRowIdsList();
            for (int i = 0; i < rowIds.size(); i++) {
                if (i >= pbResponse.getMetaUpdateStatus().getMetaUpdateTimesCount()) {
                    break;
                }
                int idx = rowIds.get(i);
                if (idx < request.getRows().size()) {
                    long updateTimeInSec = ((long) (pbResponse.getMetaUpdateStatus().getMetaUpdateTimes(i))) & 0xffffffffL;
                    if (updateTimeInSec > 0) {
                        String cacheKey = request.getRows().get(idx).getTimeseriesKey().buildMetaCacheKey(request.getTimeseriesTableName());
                        Long timeInCache = timeseriesMetaCache.getIfPresent(cacheKey);
                        if (timeInCache == null || timeInCache < updateTimeInSec) {
                            timeseriesMetaCache.put(cacheKey, updateTimeInSec);
                        }
                    }
                }
            }
        }
        return response;
    }

    public static SortedMap<String, String> parseTagsOrAttrs(String tagsStr) {
        SortedMap<String, String> tags = new TreeMap<String, String>();
        if (tagsStr.isEmpty()) {
            return tags;
        }
        if (tagsStr.length() < 2 || tagsStr.charAt(0) != '[' || tagsStr.charAt(tagsStr.length() - 1) != ']') {
            throw new ClientException("invalid tags or attributes string: " + tagsStr);
        }
        int keyStart = -1;
        int valueStart = -1;
        for (int i = 1; i < tagsStr.length() - 1; i++) {
            if (tagsStr.charAt(i) != '"') {
                throw new ClientException("invalid tags or attributes string: " + tagsStr);
            }
            keyStart = ++i;
            while ((i < tagsStr.length() - 1) && (tagsStr.charAt(i) != '=') && (tagsStr.charAt(i) != '"')) {
                i++;
            }
            if (tagsStr.charAt(i) != '=') {
                throw new ClientException("invalid tags or attributes string: " + tagsStr);
            }
            valueStart = ++i;
            while ((i < tagsStr.length() - 1) && (tagsStr.charAt(i) != '"')) {
                i++;
            }
            if (tagsStr.charAt(i) != '"') {
                throw new ClientException("invalid tags or attributes string: " + tagsStr);
            }
            tags.put(tagsStr.substring(keyStart, valueStart - 1), tagsStr.substring(valueStart, i));
            i += 1;
            if ((i < tagsStr.length() - 1) && (tagsStr.charAt(i) != ',')) {
                throw new ClientException("invalid tags or attributes string: " + tagsStr);
            }
        }
        return tags;
    }

    public static SortedMap<String, String> parseTags(List<Timeseries.TimeseriesTag> tags) {
        SortedMap<String, String> result = new TreeMap<String, String>();
        for (Timeseries.TimeseriesTag tag : tags) {
            result.put(tag.getName(), tag.getValue());
        }
        return result;
    }

    public static String convertColumnName(String colName) {
        for (int i = 0; i < colName.length(); i++) {
            if (colName.charAt(i) == ':') {
                return colName.substring(0, i);
            }
        }
        throw new ClientException("unexpect column name: " + colName);
    }

    public static TimeseriesRow parseRowFromPlainbuffer(PlainBufferRow plainBufferRow) throws IOException {
        List<PlainBufferCell> primaryKeys = plainBufferRow.getPrimaryKey();
        int timeseriesKeyCount = 0;
        String measurement = "", source = "", tagsStr = "";
        long time = -1;
        SortedMap<String, String> tags = new TreeMap<String, String>();
        for (int i = 0; i < primaryKeys.size(); i++) {
            PlainBufferCell pkCell = primaryKeys.get(i);
            if (pkCell.getCellName().equals(MEASUREMENT_COLUMN_NAME)) {
                measurement = pkCell.getPkCellValue().asString();
            } else if (pkCell.getCellName().equals(DATASOURCE_COLUMN_NAME)) {
                source = pkCell.getPkCellValue().asString();
            } else if (pkCell.getCellName().equals(TAGS_COLUMN_NAME)) {
                tagsStr = pkCell.getPkCellValue().asString();
            } else if (pkCell.getCellName().equals(TIME_COLUMN_NAME)) {
                timeseriesKeyCount = i;
                time = pkCell.getPkCellValue().asLong();
                break;
            } else if (!pkCell.getCellName().equals(HASH_COLUMN_NAME)) {
                tags.put(pkCell.getCellName(), pkCell.getPkCellValue().asString());
            }
        }
        if (!tagsStr.isEmpty()) {
            tags.putAll(parseTagsOrAttrs(tagsStr));
        }
        if (time == -1) {
            throw new ClientException("time column not found in timeseries row");
        }
        TimeseriesRow row = new TimeseriesRow(new TimeseriesKey(measurement, source, tags), time);
        for (PlainBufferCell cell : plainBufferRow.getCells()) {
            row.addField(convertColumnName(cell.getCellName()), cell.getCellValue());
        }
        for (PlainBufferCell cell : plainBufferRow.getPrimaryKey().subList(timeseriesKeyCount + 1, plainBufferRow.getPrimaryKey().size())) {
            row.addField(cell.getCellName(), cell.getPkCellValue().toColumnValue());
        }
        return row;
    }

    public static TimeseriesRow parseRowFromPlainbuffer(PlainBufferRow plainBufferRow, TimeseriesKey key) throws IOException {
        long time = -1;
        List<PlainBufferCell> primaryKeys = plainBufferRow.getPrimaryKey();
        int pkIndex = 0;
        for (; pkIndex < primaryKeys.size(); pkIndex++) {
            if (primaryKeys.get(pkIndex).getCellName().equals(TIME_COLUMN_NAME)) {
                time = primaryKeys.get(pkIndex).getPkCellValue().asLong();
                break;
            }
        }
        pkIndex++;
        if (time == -1) {
            throw new ClientException("time column not found in timeseries row");
        }
        TimeseriesRow row = new TimeseriesRow(key, time);
        for (PlainBufferCell cell : plainBufferRow.getCells()) {
            row.addField(convertColumnName(cell.getCellName()), cell.getCellValue());
        }
        for (; pkIndex < primaryKeys.size(); pkIndex++) {
            row.addField(primaryKeys.get(pkIndex).getCellName(), primaryKeys.get(pkIndex).getPkCellValue().toColumnValue());
        }
        return row;
    }

    public static GetTimeseriesDataResponse createGetTimeseriesDataResponse(
            ResponseContentWithMeta meta, Timeseries.GetTimeseriesDataResponse pbResponse) {
        GetTimeseriesDataResponse response = new GetTimeseriesDataResponse(meta.getMeta());
        if (pbResponse.hasRowsData() && !pbResponse.getRowsData().isEmpty()) {
            try {
                PlainBufferCodedInputStream inputStream = new PlainBufferCodedInputStream(
                        new PlainBufferInputStream(pbResponse.getRowsData().asReadOnlyByteBuffer()));
                List<PlainBufferRow> pbRows = inputStream.readRowsWithHeader();
                List<TimeseriesRow> rows = new ArrayList<TimeseriesRow>(pbRows.size());
                for (int i = 0; i < pbRows.size(); i++) {
                    if (i > 0) {
                        // reuse timeseries key
                        rows.add(parseRowFromPlainbuffer(pbRows.get(i), rows.get(0).getTimeseriesKey()));
                    } else {
                        rows.add(parseRowFromPlainbuffer(pbRows.get(i)));
                    }
                }
                response.setRows(rows);
            } catch (IOException e) {
                throw new ClientException("Failed to parse get timeseries data response.", e);
            }
        } else {
            response.setRows(new ArrayList<TimeseriesRow>());
        }
        if (pbResponse.hasNextToken()) {
            response.setNextToken(pbResponse.getNextToken().toByteArray());
        }
        return response;
    }

    public static TimeseriesMeta parseTimeseriesMeta(Timeseries.TimeseriesMeta pbMeta) {
        TimeseriesKey timeseriesKey = new TimeseriesKey(pbMeta.getTimeSeriesKey().getMeasurement(),
                pbMeta.getTimeSeriesKey().getSource(), parseTags(pbMeta.getTimeSeriesKey().getTagListList()));
        TimeseriesMeta meta = new TimeseriesMeta(timeseriesKey);
        if (pbMeta.hasAttributes()) {
            meta.setAttributes(parseTagsOrAttrs(pbMeta.getAttributes()));
        }
        if (pbMeta.hasUpdateTime()) {
            meta.setUpdateTimeInUs(pbMeta.getUpdateTime());
        }
        return meta;
    }

    public static QueryTimeseriesMetaResponse createQueryTimeseriesMetaResponse(
            ResponseContentWithMeta meta, Timeseries.QueryTimeseriesMetaResponse pbResponse) {
        QueryTimeseriesMetaResponse response = new QueryTimeseriesMetaResponse(meta.getMeta());
        List<TimeseriesMeta> timeseriesMetas = new ArrayList<TimeseriesMeta>(pbResponse.getTimeseriesMetasCount());
        for (int i = 0; i < pbResponse.getTimeseriesMetasCount(); i++) {
            timeseriesMetas.add(parseTimeseriesMeta(pbResponse.getTimeseriesMetas(i)));
        }
        response.setTimeseriesMetas(timeseriesMetas);
        if (pbResponse.hasTotalHit()) {
            response.setTotalHits(pbResponse.getTotalHit());
        }
        if (pbResponse.hasNextToken()) {
            response.setNextToken(pbResponse.getNextToken().toByteArray());
        }
        return response;
    }

    public static DeleteTimeseriesTableResponse createDeleteTimeseriesTableResponse(
            ResponseContentWithMeta response, Timeseries.DeleteTimeseriesTableResponse pbResponse) {
        return new DeleteTimeseriesTableResponse(response.getMeta());
    }

    public static UpdateTimeseriesTableResponse createUpdateTimeseriesTableResponse(
            ResponseContentWithMeta response, Timeseries.UpdateTimeseriesTableResponse pbResponse) {
        return new UpdateTimeseriesTableResponse(response.getMeta());
    }

    public static ListTimeseriesTableResponse createListTimeseriesTableResponse(
            ResponseContentWithMeta response, Timeseries.ListTimeseriesTableResponse pbResponse) {
        ListTimeseriesTableResponse result = new ListTimeseriesTableResponse(response.getMeta());
        List<TimeseriesTableMeta> timeseriesTableMetas = new LinkedList<TimeseriesTableMeta>();

        for (Timeseries.TimeseriesTableMeta meta : pbResponse.getTableMetasList()) {
            TimeseriesTableOptions option = new TimeseriesTableOptions(meta.getTableOptions().getTimeToLive());
            TimeseriesTableMeta _meta = new TimeseriesTableMeta(meta.getTableName(), option, meta.getStatus());
            for (String timeseriesKey : meta.getTimeseriesKeySchemaList()) {
                _meta.addTimeseriesKey(timeseriesKey);
            }
            for (OtsInternalApi.PrimaryKeySchema fieldPrimaryKeySchema : meta.getFieldPrimaryKeySchemaList()) {
                _meta.addFieldPrimaryKey(fieldPrimaryKeySchema.getName(), OTSProtocolParser.toPrimaryKeyType(fieldPrimaryKeySchema.getType()));
            }
            timeseriesTableMetas.add(_meta);
        }
        result.setTimeseriesTableMetas(timeseriesTableMetas);

        return result;
    }

    public static DescribeTimeseriesTableResponse createDescribeTimeseriesTableResponse(
            ResponseContentWithMeta response, Timeseries.DescribeTimeseriesTableResponse pbResponse) {
        DescribeTimeseriesTableResponse result = new DescribeTimeseriesTableResponse(response.getMeta());
        Timeseries.TimeseriesTableMeta meta = pbResponse.getTableMeta();

        TimeseriesTableMeta _meta = new TimeseriesTableMeta(meta.getTableName());

        TimeseriesTableOptions timeseriesTableOptions = new TimeseriesTableOptions();
        timeseriesTableOptions.setTimeToLive(meta.getTableOptions().getTimeToLive());
        _meta.setTimeseriesTableOptions(timeseriesTableOptions);

        _meta.setStatus((meta.getStatus()));

        if (meta.hasMetaOptions()) {
            Timeseries.TimeseriesMetaOptions pbMetaOptions = meta.getMetaOptions();
            TimeseriesMetaOptions metaOptions = new TimeseriesMetaOptions();
            if (pbMetaOptions.hasMetaTimeToLive()) {
                metaOptions.setMetaTimeToLive(pbMetaOptions.getMetaTimeToLive());
            }
            if (pbMetaOptions.hasAllowUpdateAttributes()) {
                metaOptions.setAllowUpdateAttributes(pbMetaOptions.getAllowUpdateAttributes());
            }
            _meta.setTimeseriesMetaOptions(metaOptions);
        }

        for (int i = 0; i < meta.getTimeseriesKeySchemaCount(); i++) {
            String timeseriesKey = meta.getTimeseriesKeySchema(i);
            _meta.addTimeseriesKey(timeseriesKey);
        }
        for (int i = 0; i < meta.getFieldPrimaryKeySchemaCount(); i++) {
            OtsInternalApi.PrimaryKeySchema fieldPrimaryKeySchema = meta.getFieldPrimaryKeySchema(i);
            _meta.addFieldPrimaryKey(fieldPrimaryKeySchema.getName(), OTSProtocolParser.toPrimaryKeyType(fieldPrimaryKeySchema.getType()));
        }

        result.setTimeseriesTableMeta(_meta);

        if (!pbResponse.getAnalyticalStoresList().isEmpty()) {
            List<TimeseriesAnalyticalStore> analyticalStores = new ArrayList<TimeseriesAnalyticalStore>();
            for (Timeseries.TimeseriesAnalyticalStore pbAnalyticalStore : pbResponse.getAnalyticalStoresList()) {
                TimeseriesAnalyticalStore analyticalStore = new TimeseriesAnalyticalStore(pbAnalyticalStore.getStoreName());
                if (pbAnalyticalStore.hasTimeToLive()) {
                    analyticalStore.setTimeToLive(pbAnalyticalStore.getTimeToLive());
                }
                if (pbAnalyticalStore.hasSyncOption()) {
                    switch (pbAnalyticalStore.getSyncOption()) {
                        case SYNC_TYPE_FULL:
                            analyticalStore.setSyncOption(AnalyticalStoreSyncType.SYNC_TYPE_FULL);
                            break;
                        case SYNC_TYPE_INCR:
                            analyticalStore.setSyncOption(AnalyticalStoreSyncType.SYNC_TYPE_INCR);
                            break;
                    }
                }
                analyticalStores.add(analyticalStore);
            }
            result.setAnalyticalStores(analyticalStores);
        }

        if (!pbResponse.getLastpointIndexesList().isEmpty()) {
            List<TimeseriesLastpointIndex> lastpointIndexes = new ArrayList<TimeseriesLastpointIndex>();
            for (Timeseries.TimeseriesLastpointIndex pbLastpointIndex : pbResponse.getLastpointIndexesList()) {
                TimeseriesLastpointIndex lastpointIndex = new TimeseriesLastpointIndex(pbLastpointIndex.getIndexTableName());
                lastpointIndexes.add(lastpointIndex);
            }
            result.setLastpointIndexes(lastpointIndexes);
        }
        return result;
    }

    public static UpdateTimeseriesMetaResponse createUpdateTimeseriesMetaResponse(
            ResponseContentWithMeta response, Timeseries.UpdateTimeseriesMetaResponse pbResponse) {
        UpdateTimeseriesMetaResponse result = new UpdateTimeseriesMetaResponse(response.getMeta());
        if (pbResponse.getFailedRowsCount() > 0) {
            List<UpdateTimeseriesMetaResponse.FailedRowResult> failedRowResultList =
                    new ArrayList<UpdateTimeseriesMetaResponse.FailedRowResult>();
            for (int i = 0; i < pbResponse.getFailedRowsCount(); i++) {
                Timeseries.FailedRowInfo failedRowInfo = pbResponse.getFailedRows(i);
                failedRowResultList.add(new UpdateTimeseriesMetaResponse.FailedRowResult(
                        failedRowInfo.getRowIndex(),
                        new Error(failedRowInfo.getErrorCode(), failedRowInfo.getErrorMessage())));
            }
            result.setFailedRows(failedRowResultList);
        }
        return result;
    }

    public static DeleteTimeseriesMetaResponse createDeleteTimeseriesMetaResponse(
            ResponseContentWithMeta response, Timeseries.DeleteTimeseriesMetaResponse pbResponse) {
        DeleteTimeseriesMetaResponse result = new DeleteTimeseriesMetaResponse(response.getMeta());
        if (pbResponse.getFailedRowsCount() > 0) {
            List<DeleteTimeseriesMetaResponse.FailedRowResult> failedRowResultList =
                    new ArrayList<DeleteTimeseriesMetaResponse.FailedRowResult>();
            for (int i = 0; i < pbResponse.getFailedRowsCount(); i++) {
                Timeseries.FailedRowInfo failedRowInfo = pbResponse.getFailedRows(i);
                failedRowResultList.add(new DeleteTimeseriesMetaResponse.FailedRowResult(
                        failedRowInfo.getRowIndex(),
                        new Error(failedRowInfo.getErrorCode(), failedRowInfo.getErrorMessage())));
            }
            result.setFailedRows(failedRowResultList);
        }
        return result;
    }

    public static SplitTimeseriesScanTaskResponse createSplitTimeseriesScanTaskResponse(
            ResponseContentWithMeta response, Timeseries.SplitTimeseriesScanTaskResponse pbResponse) {
        SplitTimeseriesScanTaskResponse result = new SplitTimeseriesScanTaskResponse(response.getMeta());
        List<TimeseriesScanSplitInfo> splitInfos = new ArrayList<TimeseriesScanSplitInfo>(pbResponse.getSplitInfosCount());
        for (int i = 0; i < pbResponse.getSplitInfosCount(); i++) {
            splitInfos.add(new TimeseriesScanSplitInfo(pbResponse.getSplitInfos(i).toByteArray()));
        }
        result.setSplitInfos(splitInfos);
        return result;
    }

    public static ScanTimeseriesDataResponse createScanTimeseriesDataResponse(
            ResponseContentWithMeta meta, Timeseries.ScanTimeseriesDataResponse pbResponse) {
        ScanTimeseriesDataResponse response = new ScanTimeseriesDataResponse(meta.getMeta());
        if (!pbResponse.hasDataSerializeType()) {
            throw new ClientException("missing data serialize type in response");
        }
        if (!pbResponse.getDataSerializeType().equals(Timeseries.RowsSerializeType.RST_PLAIN_BUFFER)) {
            throw new ClientException("unsupported data serialize type");
        }
        if (pbResponse.hasData() && !pbResponse.getData().isEmpty()) {
            try {
                PlainBufferCodedInputStream inputStream = new PlainBufferCodedInputStream(
                        new PlainBufferInputStream(pbResponse.getData().asReadOnlyByteBuffer()));
                List<PlainBufferRow> pbRows = inputStream.readRowsWithHeader();
                List<TimeseriesRow> rows = new ArrayList<TimeseriesRow>(pbRows.size());
                for (int i = 0; i < pbRows.size(); i++) {
                    rows.add(parseRowFromPlainbuffer(pbRows.get(i)));
                }
                response.setRows(rows);
            } catch (IOException e) {
                throw new ClientException("Failed to parse get timeseries data response.", e);
            }
        } else {
            response.setRows(new ArrayList<TimeseriesRow>());
        }
        if (pbResponse.hasNextToken() && !pbResponse.getNextToken().isEmpty()) {
            response.setNextToken(pbResponse.getNextToken().toByteArray());
        }
        return response;
    }

    public static CreateTimeseriesAnalyticalStoreResponse createCreateTimeseriesAnalyticalStoreResponse(
            ResponseContentWithMeta response, Timeseries.CreateTimeseriesAnalyticalStoreResponse pbResponse) {
        return new CreateTimeseriesAnalyticalStoreResponse(response.getMeta());
    }

    public static UpdateTimeseriesAnalyticalStoreResponse createUpdateTimeseriesAnalyticalStoreResponse(
            ResponseContentWithMeta response, Timeseries.UpdateTimeseriesAnalyticalStoreResponse pbResponse) {
        return new UpdateTimeseriesAnalyticalStoreResponse(response.getMeta());
    }

    public static DeleteTimeseriesAnalyticalStoreResponse createDeleteTimeseriesAnalyticalStoreResponse(
            ResponseContentWithMeta response, Timeseries.DeleteTimeseriesAnalyticalStoreResponse pbResponse) {
        return new DeleteTimeseriesAnalyticalStoreResponse(response.getMeta());
    }

    public static DescribeTimeseriesAnalyticalStoreResponse createDescribeTimeseriesAnalyticalStoreResponse(
            ResponseContentWithMeta meta, Timeseries.DescribeTimeseriesAnalyticalStoreResponse pbResponse) {
        DescribeTimeseriesAnalyticalStoreResponse response = new DescribeTimeseriesAnalyticalStoreResponse(meta.getMeta());

        if (pbResponse.hasAnalyticalStore()) {
            Timeseries.TimeseriesAnalyticalStore pbAnalyticalStore = pbResponse.getAnalyticalStore();
            TimeseriesAnalyticalStore analyticalStore = new TimeseriesAnalyticalStore(pbAnalyticalStore.getStoreName());
            if (pbAnalyticalStore.hasTimeToLive()) {
                analyticalStore.setTimeToLive(pbAnalyticalStore.getTimeToLive());
            }
            if (pbAnalyticalStore.hasSyncOption()) {
                switch (pbAnalyticalStore.getSyncOption()) {
                    case SYNC_TYPE_FULL:
                        analyticalStore.setSyncOption(AnalyticalStoreSyncType.SYNC_TYPE_FULL);
                        break;
                    case SYNC_TYPE_INCR:
                        analyticalStore.setSyncOption(AnalyticalStoreSyncType.SYNC_TYPE_INCR);
                        break;
                }
            }
            response.setAnalyticalStore(analyticalStore);
        }

        if (pbResponse.hasStorageSize()) {
            Timeseries.AnalyticalStoreStorageSize pbStorageSize = pbResponse.getStorageSize();
            AnalyticalStoreStorageSize storageSize = new AnalyticalStoreStorageSize();
            if (pbStorageSize.hasSize()) {
                storageSize.setSizeInBytes(pbStorageSize.getSize());
            }
            if (pbStorageSize.hasTimestamp()) {
                storageSize.setTimestamp(pbStorageSize.getTimestamp());
            }
            response.setStorageSize(storageSize);
        }

        if (pbResponse.hasSyncStat()) {
            Timeseries.AnalyticalStoreSyncStat pbSyncStat = pbResponse.getSyncStat();
            AnalyticalStoreSyncStat syncStat = new AnalyticalStoreSyncStat();
            if (pbSyncStat.hasCurrentSyncTimestamp()) {
                syncStat.setCurrentSyncTimestamp(pbSyncStat.getCurrentSyncTimestamp());
            }
            if (pbSyncStat.hasSyncPhase()) {
                switch (pbSyncStat.getSyncPhase()) {
                    case SYNC_TYPE_FULL:
                        syncStat.setSyncPhase(AnalyticalStoreSyncType.SYNC_TYPE_FULL);
                        break;
                    case SYNC_TYPE_INCR:
                        syncStat.setSyncPhase(AnalyticalStoreSyncType.SYNC_TYPE_INCR);
                        break;
                }
            }
            response.setSyncStat(syncStat);
        }
        return response;
    }

    public static CreateTimeseriesLastpointIndexResponse createCreateTimeseriesLastpointIndexResponse(
            ResponseContentWithMeta response, Timeseries.CreateTimeseriesLastpointIndexResponse pbResponse) {
        return new CreateTimeseriesLastpointIndexResponse(response.getMeta());
    }

    public static DeleteTimeseriesLastpointIndexResponse createDeleteTimeseriesLastpointIndexResponse(
            ResponseContentWithMeta response, Timeseries.DeleteTimeseriesLastpointIndexResponse pbResponse) {
        return new DeleteTimeseriesLastpointIndexResponse(response.getMeta());
    }
}
