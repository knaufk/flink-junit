package com.github.knaufk.flinkjunit;

public class FlinkJUnitException extends RuntimeException {

    public FlinkJUnitException() {
    }

    public FlinkJUnitException(final String message) {
        super(message);
    }

    public FlinkJUnitException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
