package com.alicloud.openservices.tablestore.model.tunnel;

public class StreamTunnelConfig {
    /**
     * StartOffsetFlag(EARLIEST or LATEST)
     */
    private StartOffsetFlag flag;
    /**
     * StartOffset(in MillSecond), valid range: [CurrentSystemTime - StreamExpiration + 5 minute, CurrentSystemTime)
     */
    private long startOffset;
    /**
     * EndOffset(in MillSecond)
     */
    private long endOffset;

    public StreamTunnelConfig() {
        this(StartOffsetFlag.LATEST);
    }

    public StreamTunnelConfig(StartOffsetFlag flag) {
        this.flag = flag;
    }

    public StreamTunnelConfig(long startOffset, long endOffset) {
        this(StartOffsetFlag.LATEST, startOffset, endOffset);
    }

    public StreamTunnelConfig(StartOffsetFlag flag, long startOffset, long endOffset) {
        this.flag = flag;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public StartOffsetFlag getFlag() {
        return flag;
    }

    public void setFlag(StartOffsetFlag flag) {
        this.flag = flag;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("StartOffsetFlag: " + flag.name() + ", ");
        sb.append("StartOffset: " + startOffset + ", ");
        sb.append("EndOffset: " + endOffset);
        return sb.toString();
    }
}

