package com.alicloud.openservices.tablestore.core.protocol.timeseries;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.PlainBufferCell;
import com.alicloud.openservices.tablestore.core.protocol.PlainBufferCodedInputStream;
import com.alicloud.openservices.tablestore.core.protocol.PlainBufferInputStream;
import com.alicloud.openservices.tablestore.core.protocol.PlainBufferRow;
import com.alicloud.openservices.tablestore.model.Error;
import com.alicloud.openservices.tablestore.model.TimeseriesMetaOptions;
import com.alicloud.openservices.tablestore.model.TimeseriesTableMeta;
import com.alicloud.openservices.tablestore.model.TimeseriesTableOptions;
import com.alicloud.openservices.tablestore.model.timeseries.*;
import com.google.common.cache.Cache;

import java.io.IOException;
import java.util.*;

public class TimeseriesResponseFactory {

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
        for (int i = 1; i < tagsStr.length()-1; i++) {
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
            while ((i < tagsStr.length() - 1) &&(tagsStr.charAt(i) != '"')) {
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

    public static String convertColumnName(String colName) {
        for (int i = 0; i < colName.length(); i++) {
            if (colName.charAt(i) == ':') {
                return colName.substring(0, i);
            }
        }
        throw new ClientException("unexpect column name: " + colName);
    }

    public static TimeseriesRow parseRowFromPlainbuffer(PlainBufferRow plainBufferRow) {
        String measurement = plainBufferRow.getPrimaryKey().get(1).getPkCellValue().asString();
        String source = plainBufferRow.getPrimaryKey().get(2).getPkCellValue().asString();
        String tagsStr = plainBufferRow.getPrimaryKey().get(3).getPkCellValue().asString();
        long time = plainBufferRow.getPrimaryKey().get(4).getPkCellValue().asLong();
        TimeseriesRow row = new TimeseriesRow(new TimeseriesKey(measurement, source, parseTagsOrAttrs(tagsStr)), time);
        for (PlainBufferCell cell : plainBufferRow.getCells()) {
            row.addField(convertColumnName(cell.getCellName()), cell.getCellValue());
        }
        return row;
    }

    public static TimeseriesRow parseRowFromPlainbuffer(PlainBufferRow plainBufferRow, TimeseriesKey key) {
        long time = plainBufferRow.getPrimaryKey().get(4).getPkCellValue().asLong();
        TimeseriesRow row = new TimeseriesRow(key, time);
        for (PlainBufferCell cell : plainBufferRow.getCells()) {
            row.addField(convertColumnName(cell.getCellName()), cell.getCellValue());
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
            pbMeta.getTimeSeriesKey().getSource(), parseTagsOrAttrs(pbMeta.getTimeSeriesKey().getTags()));
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

        for(Timeseries.TimeseriesTableMeta meta: pbResponse.getTableMetasList()) {
            TimeseriesTableOptions option = new TimeseriesTableOptions(meta.getTableOptions().getTimeToLive());
            timeseriesTableMetas.add(new TimeseriesTableMeta(meta.getTableName(), option, meta.getStatus()));
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
}
