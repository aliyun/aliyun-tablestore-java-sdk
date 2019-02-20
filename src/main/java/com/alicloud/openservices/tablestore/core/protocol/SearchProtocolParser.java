package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.CapacityUnit;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.ReservedThroughput;
import com.alicloud.openservices.tablestore.model.search.*;
import com.alicloud.openservices.tablestore.model.search.sort.FieldSort;
import com.alicloud.openservices.tablestore.model.search.sort.PrimaryKeySort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import com.alicloud.openservices.tablestore.model.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.List;

public class SearchProtocolParser {

    public static FieldType toFieldType(Search.FieldType fieldType) {
        switch (fieldType) {
            case LONG:
                return FieldType.LONG;
            case DOUBLE:
                return FieldType.DOUBLE;
            case BOOLEAN:
                return FieldType.BOOLEAN;
            case KEYWORD:
                return FieldType.KEYWORD;
            case TEXT:
                return FieldType.TEXT;
            case NESTED:
                return FieldType.NESTED;
            case GEO_POINT:
                return FieldType.GEO_POINT;
            default:
                throw new IllegalArgumentException("Unknown fieldType: " + fieldType.name());
        }
    }

    public static IndexOptions toIndexOptions(Search.IndexOptions indexOptions) {
        switch (indexOptions) {
            case DOCS:
                return IndexOptions.DOCS;
            case FREQS:
                return IndexOptions.FREQS;
            case POSITIONS:
                return IndexOptions.POSITIONS;
            case OFFSETS:
                return IndexOptions.OFFSETS;
            default:
                throw new IllegalArgumentException("Unknown indexOptions: " + indexOptions.name());
        }
    }

    public static FieldSchema toFieldSchema(Search.FieldSchema fieldSchema) {
        FieldSchema result = new FieldSchema(fieldSchema.getFieldName(),
                toFieldType(fieldSchema.getFieldType()));
        if (fieldSchema.hasIndex()) {
            result.setIndex(fieldSchema.getIndex());
        } else {
            result.setIndex(false);
        }
        if (fieldSchema.hasDocValues()) {
            result.setEnableSortAndAgg(fieldSchema.getDocValues());
        } else {
            result.setEnableSortAndAgg(false);
        }
        if (fieldSchema.hasStore()) {
            result.setStore(fieldSchema.getStore());
        } else {
            result.setStore(false);
        }
        if (fieldSchema.hasIsArray()) {
            result.setIsArray(fieldSchema.getIsArray());
        } else {
            result.setIsArray(false);
        }
        if (fieldSchema.hasIndexOptions()) {
            result.setIndexOptions(toIndexOptions(fieldSchema.getIndexOptions()));
        }
        if (fieldSchema.hasAnalyzer()) {
            result.setAnalyzer(FieldSchema.Analyzer.fromString(fieldSchema.getAnalyzer()));
        }
        if (fieldSchema.getFieldSchemasList() != null) {
            List<FieldSchema> subSchemas = new ArrayList<FieldSchema>();
            for (Search.FieldSchema subSchema : fieldSchema.getFieldSchemasList()) {
                subSchemas.add(toFieldSchema(subSchema));
            }
            result.setSubFieldSchemas(subSchemas);
        }
        return result;
    }

    public static IndexSetting toIndexSetting(Search.IndexSetting indexSetting) {
        IndexSetting result = new IndexSetting();
        if (indexSetting.getRoutingFieldsCount() > 0) {
            result.setRoutingFields(indexSetting.getRoutingFieldsList());
        }
        return result;
    }

    public static Sort toIndexSort(Search.Sort sort) {
        if (sort.getSorterCount() == 0) {
            return null;
        }
        List<Sort.Sorter> sorters = new ArrayList<Sort.Sorter>();
        for (Search.Sorter sorter : sort.getSorterList()) {
            if (sorter.hasFieldSort()) {
                Search.FieldSort pbFieldSort = sorter.getFieldSort();
                FieldSort fieldSort = new FieldSort(pbFieldSort.getFieldName());
                if (pbFieldSort.hasOrder()) {
                    if (pbFieldSort.getOrder().equals(Search.SortOrder.SORT_ORDER_ASC)) {
                        fieldSort.setOrder(SortOrder.ASC);
                    } else {
                        fieldSort.setOrder(SortOrder.DESC);
                    }
                }
                sorters.add(fieldSort);
            } else if (sorter.hasPkSort()) {
                Search.PrimaryKeySort pbPkSort = sorter.getPkSort();
                PrimaryKeySort pkSort = new PrimaryKeySort();
                if (pbPkSort.hasOrder()) {
                    if (pbPkSort.getOrder().equals(Search.SortOrder.SORT_ORDER_ASC)) {
                        pkSort.setOrder(SortOrder.ASC);
                    } else {
                        pkSort.setOrder(SortOrder.DESC);
                    }
                }
                sorters.add(pkSort);
            } else {
                throw new ClientException("failed to parse index_sort in response");
            }
        }
        return new Sort(sorters);
    }

    public static IndexSchema toIndexSchema(Search.IndexSchema indexSchema) {
        IndexSchema result = new IndexSchema();
        result.setIndexSetting(toIndexSetting(indexSchema.getIndexSetting()));
        List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
        for (Search.FieldSchema fieldSchema : indexSchema.getFieldSchemasList()) {
            fieldSchemas.add(toFieldSchema(fieldSchema));
        }
        result.setFieldSchemas(fieldSchemas);
        if (indexSchema.hasIndexSort()) {
            result.setIndexSort(toIndexSort(indexSchema.getIndexSort()));
        }
        return result;
    }

    public static SyncStat toSyncStat(Search.SyncStat syncStat) {
        SyncStat result = new SyncStat();
        if (!syncStat.hasSyncPhase()) {
            throw new ClientException("missing [SyncPhase] in SyncStat");
        }
        switch (syncStat.getSyncPhase()) {
            case FULL:
                result.setSyncPhase(SyncStat.SyncPhase.FULL);
                break;
            case INCR:
                result.setSyncPhase(SyncStat.SyncPhase.INCR);
                break;
            default:
                throw new ClientException("unknown SyncPhase: " + syncStat.getSyncPhase().name());
        }
        if (syncStat.hasCurrentSyncTimestamp()) {
            result.setCurrentSyncTimestamp(syncStat.getCurrentSyncTimestamp());
        }
        return result;
    }

    public static MeteringInfo toMeteringInfo(Search.MeteringInfo meteringInfo) {
        MeteringInfo result = new MeteringInfo();
        if (meteringInfo.hasReservedReadCu()) {
            result.setReservedThroughput(new ReservedThroughput(
                    new CapacityUnit((int) meteringInfo.getReservedReadCu(), 0)));
        }
        if (meteringInfo.hasStorageSize()) {
            result.setStorageSize(meteringInfo.getStorageSize());
        }
        if (meteringInfo.hasDocCount()) {
            result.setDocCount(meteringInfo.getDocCount());
        }
        if (meteringInfo.hasTimestamp()) {
            result.setTimestamp(meteringInfo.getTimestamp());
        }
        return result;
    }
}
