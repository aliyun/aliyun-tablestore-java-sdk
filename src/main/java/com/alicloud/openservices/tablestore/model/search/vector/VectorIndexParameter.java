package com.alicloud.openservices.tablestore.model.search.vector;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.google.protobuf.ByteString;

public interface VectorIndexParameter extends Jsonizable {
    ByteString serialize();
}
