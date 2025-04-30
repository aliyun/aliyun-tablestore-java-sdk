package com.alicloud.openservices.tablestore.model;

import java.util.List;

public class ListStreamResponse extends Response {

    /**
     * The list of Streams returned by the request.
     */
    private List<Stream> streams;

    public ListStreamResponse() {
    }

    public ListStreamResponse(Response meta) {
        super(meta);
    }

    /**
     * Get the list of Streams
     * @return List of Streams
     */
    public List<Stream> getStreams() {
        return streams;
    }

    public void setStreams(List<Stream> streams) {
        this.streams = streams;
    }
}
