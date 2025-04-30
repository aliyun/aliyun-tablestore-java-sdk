package com.alicloud.openservices.tablestore.core.protocol;

public class PlainBufferExtension {

    private PlainBufferSequenceInfo sequenceInfo;


    public PlainBufferExtension()
    {
        this.sequenceInfo = new PlainBufferSequenceInfo();
    }
    public void setSequenceInfo(PlainBufferSequenceInfo sequenceInfo) {
        this.sequenceInfo = sequenceInfo;
    }

    public PlainBufferSequenceInfo getSequenceInfo() {
        return sequenceInfo;
    }

    public boolean hasSeq() {
        return sequenceInfo.getHasSeq();
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        if (hasSeq()) {
            sb.append(" SequenceInfo: {");
            sb.append(getSequenceInfo());
            sb.append("}");
        }
        return sb.toString();
    }
}
