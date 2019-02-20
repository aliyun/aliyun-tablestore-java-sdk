package com.alicloud.openservices.tablestore.tunnel.pipeline;

public class StageException extends Exception {
    private Stage<?, ?> stage;
    private Object input;

    public StageException(Stage<?, ?> stage, Object input, String message) {
        super(message);
        this.stage = stage;
        this.input = input;
    }

    public StageException(Stage<?, ?> stage, Object input, String message, Throwable cause) {
        super(message, cause);
        this.stage = stage;
        this.input = input;
    }

    @Override
    public String toString() {
        return input.toString() + super.toString();
    }
}
