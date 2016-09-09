package org.neuinfo.foundry.common.model;

/**
 * Created by bozyurt on 5/27/14.
 */
public enum Status {
    NOT_STARTED(0), IN_PROCESS(1), FINISHED(2);

    private final int code;

    private Status(int code) {
        this.code = code;
    }

    public int getCode() {
        return this.code;
    }

    public static Status fromCode(int code) {
        switch (code) {
            case 1:
                return Status.IN_PROCESS;
            case 0:
                return Status.NOT_STARTED;
            case 2:
                return Status.FINISHED;
            default:
                return null;
        }
    }
}
