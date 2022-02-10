package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.alicloud.openservices.tablestore.core.Constants.UTF8_CHARSET;

/**
 * 面向range scan场景的基于行的数据块
 */
public class PlainBufferBlockParser {

    private static Logger logger = LoggerFactory.getLogger(ResponseFactory.class);

    private ByteBuffer buffer;
    private List<Row> rows;

    public PlainBufferBlockParser(ByteBuffer buffer) {
        this.buffer = buffer;
        this.rows = new ArrayList<Row>();
        try {
            if (buffer != null) {
                PlainBufferCodedInputStream inputStream = new PlainBufferCodedInputStream(
                        new PlainBufferInputStream(buffer));
                List<PlainBufferRow> pbRows = inputStream.readRowsWithHeader();
                for (PlainBufferRow pbRow : pbRows) {
                    rows.add(PlainBufferConversion.toRow(pbRow));
                }
            }
        } catch (Exception e) {
            throw new ClientException("Failed to parse get range response.", e);
        }
    }

    public int getRowCount() {
        return rows.size();
    }

    public List<Row> getRows() {
        return rows;
    }

    public int getTotalBytes() {
        return buffer.limit();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PlainBufferBlock{\n");
        for (Row row: rows) {
            sb.append(row).append('\n');
        }
        sb.append("}");
        return sb.toString();
    }
}

