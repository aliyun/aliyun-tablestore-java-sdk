package com.alicloud.openservices.tablestore.core.protocol.timeseries;

import com.alicloud.openservices.tablestore.core.protocol.timeseries.flatbuffer.*;
import com.alicloud.openservices.tablestore.core.utils.Pair;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.PureJavaCrc32C;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.timeseries.*;
import com.google.common.cache.Cache;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.zip.Checksum;

public class TimeseriesProtocolBuilder {

    public static void checkTagKey(String s) {
        if (s.isEmpty()) {
            throw new IllegalArgumentException("empty tag key");
        }
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) >= '!' && s.charAt(i) <= '~' && s.charAt(i) != '"' && s.charAt(i) != '=') {
                // valid
            } else {
                throw new IllegalArgumentException("invalid tag key: " + s);
            }
        }
    }

    public static void checkTagValue(String s) {
        if (s.isEmpty()) {
            throw new IllegalArgumentException("empty tag value");
        }
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) != '"' && s.charAt(i) != '=') {
                // valid
            } else {
                throw new IllegalArgumentException("invalid tag value: " + s);
            }
        }
    }

    public static String buildTagsString(SortedMap<String, String> tags) {
        int capacity = 2;
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            capacity += entry.getKey().length() + entry.getValue().length() + 3;
        }
        StringBuilder sb = new StringBuilder(capacity);
        sb.append('[');
        boolean first = true;
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            checkTagKey(entry.getKey());
            checkTagValue(entry.getValue());
            if (!first) {
                sb.append(',');
            }
            sb.append('"');
            sb.append(entry.getKey());
            sb.append('=');
            sb.append(entry.getValue());
            sb.append('"');
            first = false;
        }
        sb.append(']');
        return sb.toString();
    }

    public static int buildRowToRowGroupOffset(TimeseriesRow row, FlatBufferBuilder fbb, String timeseriesTableName, Cache<String, Long> timeseriesMetaCache) {
        int fieldCount = row.getFields().size();
        byte[] fieldValueTypes = new byte[fieldCount];
        int[] fieldNameOffs = new int[fieldCount];
        int idx = 0;
        int longValueCount = 0;
        int boolValueCount = 0;
        int doubleValueCount = 0;
        int stringValueCount = 0;
        int binaryValueCount = 0;
        for (Map.Entry<String, ColumnValue> entry : row.getFields().entrySet()) {
            fieldNameOffs[idx] = fbb.createString(entry.getKey());
            switch (entry.getValue().getType()) {
                case INTEGER: {
                    fieldValueTypes[idx] = DataType.LONG;
                    longValueCount++;
                    break;
                }
                case BOOLEAN: {
                    fieldValueTypes[idx] = DataType.BOOLEAN;
                    boolValueCount++;
                    break;
                }
                case DOUBLE: {
                    fieldValueTypes[idx] = DataType.DOUBLE;
                    doubleValueCount++;
                    break;
                }
                case STRING: {
                    fieldValueTypes[idx] = DataType.STRING;
                    stringValueCount++;
                    break;
                }
                case BINARY: {
                    fieldValueTypes[idx] = DataType.BINARY;
                    binaryValueCount++;
                    break;
                }
                default:
                    throw new IllegalStateException();
            }
            idx++;
        }
        long[] longValues = new long[longValueCount];
        boolean[] boolValues = new boolean[boolValueCount];
        double[] doubleValues = new double[doubleValueCount];
        int[] strValueOffs = new int[stringValueCount];
        int[] binaryValueOffs = new int[binaryValueCount];
        longValueCount = 0;
        boolValueCount = 0;
        doubleValueCount = 0;
        stringValueCount = 0;
        binaryValueCount = 0;
        for (Map.Entry<String, ColumnValue> entry : row.getFields().entrySet()) {
            switch (entry.getValue().getType()) {
                case INTEGER: {
                    longValues[longValueCount++] = entry.getValue().asLong();
                    break;
                }
                case BOOLEAN: {
                    boolValues[boolValueCount++] = entry.getValue().asBoolean();
                    break;
                }
                case DOUBLE: {
                    doubleValues[doubleValueCount++] = entry.getValue().asDouble();
                    break;
                }
                case STRING: {
                    strValueOffs[stringValueCount++] = fbb.createString(entry.getValue().asString());
                    break;
                }
                case BINARY: {
                    binaryValueOffs[binaryValueCount++] = BytesValue.createBytesValue(fbb,
                            BytesValue.createValueVector(fbb, entry.getValue().asBinary()));
                    break;
                }
                default:
                    throw new IllegalStateException();
            }
        }
        int fieldValueOff = FieldValues.createFieldValues(fbb,
                longValueCount == 0 ? 0 : FieldValues.createLongValuesVector(fbb, longValues),
                boolValueCount == 0 ? 0 : FieldValues.createBoolValuesVector(fbb, boolValues),
                doubleValueCount == 0 ? 0 : FieldValues.createDoubleValuesVector(fbb, doubleValues),
                stringValueCount == 0 ? 0 : FieldValues.createStringValuesVector(fbb, strValueOffs),
                binaryValueCount == 0 ? 0 : FieldValues.createBinaryValuesVector(fbb, binaryValueOffs));
        int[] rowInGroupOffs = new int[1];
        Long updateTimeInSec = timeseriesMetaCache.getIfPresent(row.getTimeseriesKey().buildMetaCacheKey(timeseriesTableName));
        long updateTime = updateTimeInSec == null ? 0 : updateTimeInSec;
        rowInGroupOffs[0] = FlatBufferRowInGroup.createFlatBufferRowInGroup(fbb,
                row.getTimeseriesKey().getDataSource() == null ? fbb.createString("") : fbb.createString(row.getTimeseriesKey().getDataSource()),
                fbb.createString(row.getTimeseriesKey().buildTagsString()),
                row.getTimeInUs(), fieldValueOff, updateTime);
        return FlatBufferRowGroup.createFlatBufferRowGroup(fbb, fbb.createString(row.getTimeseriesKey().getMeasurementName()),
                FlatBufferRowGroup.createFieldNamesVector(fbb, fieldNameOffs),
                FlatBufferRowGroup.createFieldTypesVector(fbb, fieldValueTypes),
                FlatBufferRowGroup.createRowsVector(fbb, rowInGroupOffs));
    }

    private static Timeseries.TimeseriesTableOptions buildTimeseriesTableOptions(TimeseriesTableOptions timeseriesTableOptions) {

        Timeseries.TimeseriesTableOptions.Builder builder = Timeseries.TimeseriesTableOptions.newBuilder();

        if (timeseriesTableOptions.hasSetTimeToLive()) {
            builder.setTimeToLive(timeseriesTableOptions.getTimeToLive());
        }

        return builder.build();
    }

    public static Timeseries.TimeseriesTableMeta buildTimeseriesTableMeta(TimeseriesTableMeta timeseriesTableMeta) {
        Timeseries.TimeseriesTableMeta.Builder builder = Timeseries.TimeseriesTableMeta.newBuilder();

        // required string table_name = 1;
        builder.setTableName(timeseriesTableMeta.getTimeseriesTableName());

        // optional TimeseriesTableOptions table_options = 2;
        builder.setTableOptions(buildTimeseriesTableOptions(timeseriesTableMeta.getTimeseriesTableOptions()));

        return builder.build();
    }

    public static ByteBuffer buildFlatbufferRows(List<TimeseriesRow> rows, String timeseriesTableName, Cache<String, Long> timeseriesMetaCache) {
        FlatBufferBuilder fbb = new FlatBufferBuilder();
        int[] rowGroupOffs = new int[rows.size()];
        for (int i = 0; i < rows.size(); i++) {
            TimeseriesRow row = rows.get(i);
            rowGroupOffs[i] = buildRowToRowGroupOffset(row, fbb, timeseriesTableName, timeseriesMetaCache);
        }
        int rowsOffset = FlatBufferRows.createFlatBufferRows(fbb, FlatBufferRows.createRowGroupsVector(fbb, rowGroupOffs));
        fbb.finish(rowsOffset);
        return fbb.dataBuffer();
    }

    public static Timeseries.CreateTimeseriesTableRequest buildCreateTimeseriesTableRequest(CreateTimeseriesTableRequest createTimeseriesTableRequest) {

        Timeseries.CreateTimeseriesTableRequest.Builder builder = Timeseries.CreateTimeseriesTableRequest.newBuilder();

        // required TimeseriesTableMeta table_meta = 1;
        builder.setTableMeta(buildTimeseriesTableMeta(createTimeseriesTableRequest.getTimeseriesTableMeta()));

        return builder.build();
    }

    public static Timeseries.PutTimeseriesDataRequest buildPutTimeseriesDataRequest(PutTimeseriesDataRequest request, Cache<String, Long> timeseriesMetaCache) {
        Timeseries.PutTimeseriesDataRequest.Builder builder = Timeseries.PutTimeseriesDataRequest.newBuilder();
        builder.setTableName(request.getTimeseriesTableName());

        Timeseries.TimeseriesRows.Builder rowsBuilder = Timeseries.TimeseriesRows.newBuilder();
        rowsBuilder.setType(Timeseries.RowsSerializeType.RST_FLAT_BUFFER);
        ByteBuffer flatbufferRowsData = buildFlatbufferRows(request.getRows(), request.getTimeseriesTableName(), timeseriesMetaCache);
        
        Checksum crc32c = new PureJavaCrc32C();
        crc32c.update(flatbufferRowsData.array(), flatbufferRowsData.position() + flatbufferRowsData.arrayOffset(),
            flatbufferRowsData.remaining());
        int crc = (int) crc32c.getValue();
        byte[] data = new byte[flatbufferRowsData.remaining()];
        for (int i = 0; i < flatbufferRowsData.remaining(); i++) {
            data[i] = flatbufferRowsData.array()[i + flatbufferRowsData.position() + flatbufferRowsData.arrayOffset()];
        }
        rowsBuilder.setRowsData(ByteString.copyFrom(flatbufferRowsData));
        rowsBuilder.setFlatbufferCrc32C(crc);
        builder.setRowsData(rowsBuilder);
        return builder.build();
    }

    public static Timeseries.TimeseriesKey buildTimeseriesKey(TimeseriesKey timeseriesKey) {
        Timeseries.TimeseriesKey.Builder tsKeyBuilder = Timeseries.TimeseriesKey.newBuilder();
        tsKeyBuilder.setMeasurement(timeseriesKey.getMeasurementName());
        tsKeyBuilder.setSource(timeseriesKey.getDataSource());
        tsKeyBuilder.setTags(buildTagsString(timeseriesKey.getTags()));
        return tsKeyBuilder.build();
    }

    public static Timeseries.GetTimeseriesDataRequest buildGetTimeseriesDataRequest(GetTimeseriesDataRequest request) {
        Preconditions.checkNotNull(request.getTimeseriesTableName());
        Preconditions.checkNotNull(request.getTimeseriesKey());
        Preconditions.checkNotNull(request.getTimeseriesKey().getMeasurementName());
        Preconditions.checkNotNull(request.getTimeseriesKey().getDataSource());
        Preconditions.checkArgument(request.getEndTimeInUs() > 0, "time range not set");
        Preconditions.checkArgument(request.getBeginTimeInUs() < request.getEndTimeInUs(),
            "end time should be large than begin time");
        Timeseries.GetTimeseriesDataRequest.Builder builder = Timeseries.GetTimeseriesDataRequest.newBuilder();
        builder.setTableName(request.getTimeseriesTableName());
        builder.setTimeSeriesKey(buildTimeseriesKey(request.getTimeseriesKey()));
        builder.setBeginTime(request.getBeginTimeInUs());
        builder.setEndTime(request.getEndTimeInUs());
        if (request.isBackward()) {
            builder.setBackward(true);
        }
        if (!request.getFieldsToGet().isEmpty()) {
            for (Pair<String, ColumnType> field : request.getFieldsToGet()) {
                Timeseries.TimeseriesFieldsToGet.Builder fieldsToGet = Timeseries.TimeseriesFieldsToGet.newBuilder();
                Preconditions.checkStringNotNullAndEmpty(field.getFirst(), "field name is empty");
                Preconditions.checkNotNull(field.getSecond());
                fieldsToGet.setName(field.getFirst());
                switch (field.getSecond()) {
                    case INTEGER: {
                        fieldsToGet.setType(DataType.LONG);
                        break;
                    }
                    case DOUBLE: {
                        fieldsToGet.setType(DataType.DOUBLE);
                        break;
                    }
                    case STRING: {
                        fieldsToGet.setType(DataType.STRING);
                        break;
                    }
                    case BINARY: {
                        fieldsToGet.setType(DataType.BINARY);
                        break;
                    }
                    case BOOLEAN: {
                        fieldsToGet.setType(DataType.BOOLEAN);
                        break;
                    }
                    default: {
                        throw new IllegalStateException();
                    }
                }
                builder.addFieldsToGet(fieldsToGet);
            }
        }
        if (request.getNextToken() != null && request.getNextToken().length != 0) {
            builder.setToken(ByteString.copyFrom(request.getNextToken()));
        }
        if (request.getLimit() > 0) {
            builder.setLimit(request.getLimit());
        }
        return builder.build();
    }

    public static Timeseries.MetaQueryCondition buildMetaQueryCondition(MetaQueryCondition condition) {
        Timeseries.MetaQueryCondition.Builder builder = Timeseries.MetaQueryCondition.newBuilder();
        builder.setType(condition.getType());
        builder.setProtoData(condition.serialize());
        return builder.build();
    }

    public static Timeseries.QueryTimeseriesMetaRequest buildQueryTimeseriesMetaRequest(QueryTimeseriesMetaRequest request) {
        Preconditions.checkNotNull(request.getTimeseriesTableName());
        Timeseries.QueryTimeseriesMetaRequest.Builder builder = Timeseries.QueryTimeseriesMetaRequest.newBuilder();
        builder.setTableName(request.getTimeseriesTableName());
        if (request.getCondition() != null) {
            builder.setCondition(buildMetaQueryCondition(request.getCondition()));
        }
        builder.setGetTotalHit(request.isGetTotalHits());
        if (request.getNextToken() != null && request.getNextToken().length != 0) {
            builder.setToken(ByteString.copyFrom(request.getNextToken()));
        }
        if (request.getLimit() > 0) {
            builder.setLimit(request.getLimit());
        }
        return builder.build();
    }

    public static Timeseries.ListTimeseriesTableRequest buildListTimeseriesTableRequest(ListTimeseriesTableRequest request) {
        Timeseries.ListTimeseriesTableRequest.Builder builder = Timeseries.ListTimeseriesTableRequest.newBuilder();
        return builder.build();
    }

    public static Timeseries.DeleteTimeseriesTableRequest buildDeleteTimeseriesTableRequest(DeleteTimeseriesTableRequest deleteTimeseriesTableRequest) {
        Timeseries.DeleteTimeseriesTableRequest.Builder builder = Timeseries.DeleteTimeseriesTableRequest.newBuilder();

        // required String table_name = 1;
        builder.setTableName(deleteTimeseriesTableRequest.getTimeseriesTableName());

        return builder.build();
    }

    public static Timeseries.DescribeTimeseriesTableRequest buildDescribeTimeseriesTableRequest(DescribeTimeseriesTableRequest describeTimeseriesTableRequest) {
        Timeseries.DescribeTimeseriesTableRequest.Builder builder = Timeseries.DescribeTimeseriesTableRequest.newBuilder();

        // required String table_name = 1;
        builder.setTableName(describeTimeseriesTableRequest.getTimeseriesTableName());

        return builder.build();
    }

    public static Timeseries.UpdateTimeseriesTableRequest buildUpdateTimeseriesTableRequest(UpdateTimeseriesTableRequest updateTimeseriesTableRequest) {
        Timeseries.UpdateTimeseriesTableRequest.Builder builder = Timeseries.UpdateTimeseriesTableRequest.newBuilder();

        // required String table_name = 1;
        builder.setTableName(updateTimeseriesTableRequest.getTimeseriesTableName());

        // optional TimeseriesTableOptions table_options = 2;
        if (updateTimeseriesTableRequest.getTimeseriesTableOptions().hasSetTimeToLive()) {
            builder.setTableOptions(buildTimeseriesTableOptions(updateTimeseriesTableRequest.getTimeseriesTableOptions()));
        }

        return builder.build();
    }

    public static Timeseries.UpdateTimeseriesMetaRequest buildUpdateTimeseriesMetaRequest(UpdateTimeseriesMetaRequest updateTimeseriesMetaRequest) {
        Timeseries.UpdateTimeseriesMetaRequest.Builder builder = Timeseries.UpdateTimeseriesMetaRequest.newBuilder();

        builder.setTableName(updateTimeseriesMetaRequest.getTimeseriesTableName());
        if (updateTimeseriesMetaRequest.getMetas().isEmpty()) {
            throw new IllegalArgumentException("empty timeseries meta");
        }
        for (TimeseriesMeta meta : updateTimeseriesMetaRequest.getMetas()) {
            if (meta.getUpdateTimeInUs() > 0) {
                throw new IllegalArgumentException("update time can not be set");
            }
            Timeseries.TimeseriesMeta.Builder pbMeta = Timeseries.TimeseriesMeta.newBuilder();
            pbMeta.setTimeSeriesKey(buildTimeseriesKey(meta.getTimeseriesKey()));
            if (!meta.getAttributes().isEmpty()) {
                pbMeta.setAttributes(buildTagsString(meta.getAttributes()));
            }
            builder.addTimeseriesMeta(pbMeta);
        }
        return builder.build();
    }

    public static Timeseries.DeleteTimeseriesMetaRequest buildDeleteTimeseriesMetaRequest(DeleteTimeseriesMetaRequest deleteTimeseriesMetaRequest) {
        Timeseries.DeleteTimeseriesMetaRequest.Builder builder = Timeseries.DeleteTimeseriesMetaRequest.newBuilder();

        builder.setTableName(deleteTimeseriesMetaRequest.getTimeseriesTableName());
        if (deleteTimeseriesMetaRequest.getTimeseriesKeys().isEmpty()) {
            throw new IllegalArgumentException("empty timeseries key");
        }
        for (TimeseriesKey key : deleteTimeseriesMetaRequest.getTimeseriesKeys()) {
            builder.addTimeseriesKey(buildTimeseriesKey(key));
        }
        return builder.build();
    }

}
