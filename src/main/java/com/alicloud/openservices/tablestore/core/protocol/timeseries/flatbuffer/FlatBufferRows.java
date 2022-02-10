// automatically generated by the FlatBuffers compiler, do not modify

package com.alicloud.openservices.tablestore.core.protocol.timeseries.flatbuffer;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class FlatBufferRows extends Table {
  public static FlatBufferRows getRootAsFlatBufferRows(ByteBuffer _bb) { return getRootAsFlatBufferRows(_bb, new FlatBufferRows()); }
  public static FlatBufferRows getRootAsFlatBufferRows(ByteBuffer _bb, FlatBufferRows obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; vtable_start = bb_pos - bb.getInt(bb_pos); vtable_size = bb.getShort(vtable_start); }
  public FlatBufferRows __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public FlatBufferRowGroup rowGroups(int j) { return rowGroups(new FlatBufferRowGroup(), j); }
  public FlatBufferRowGroup rowGroups(FlatBufferRowGroup obj, int j) { int o = __offset(4); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int rowGroupsLength() { int o = __offset(4); return o != 0 ? __vector_len(o) : 0; }

  public static int createFlatBufferRows(FlatBufferBuilder builder,
      int row_groupsOffset) {
    builder.startObject(1);
    FlatBufferRows.addRowGroups(builder, row_groupsOffset);
    return FlatBufferRows.endFlatBufferRows(builder);
  }

  public static void startFlatBufferRows(FlatBufferBuilder builder) { builder.startObject(1); }
  public static void addRowGroups(FlatBufferBuilder builder, int rowGroupsOffset) { builder.addOffset(0, rowGroupsOffset, 0); }
  public static int createRowGroupsVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startRowGroupsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endFlatBufferRows(FlatBufferBuilder builder) {
    int o = builder.endObject();
    return o;
  }
  public static void finishFlatBufferRowsBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
  public static void finishSizePrefixedFlatBufferRowsBuffer(FlatBufferBuilder builder, int offset) { builder.finishSizePrefixed(offset); }
}

