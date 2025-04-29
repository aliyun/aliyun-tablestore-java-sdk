package com.alicloud.openservices.tablestore.reader;

public class PkWithGroup {
    public final PrimaryKeyWithTable primaryKeyWithTable;
    public final ReaderGroup readerGroup;

    public PkWithGroup(PrimaryKeyWithTable primaryKeyWithTable, ReaderGroup readerGroup) {
        this.primaryKeyWithTable = primaryKeyWithTable;
        this.readerGroup = readerGroup;
    }
}
