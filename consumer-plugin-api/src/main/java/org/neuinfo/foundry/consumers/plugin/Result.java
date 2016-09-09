package org.neuinfo.foundry.consumers.plugin;

import com.mongodb.DBObject;
import org.json.JSONObject;

/**
 * Created by bozyurt on 10/28/14.
 */
public class Result {
    private DBObject docWrapper;
    private JSONObject payload;
    private String errMessage;
    private Status status = Status.NONE;
    private OpMode opMode = OpMode.ADD_UPDATE;
    private boolean largeRecord = false;
    /** full path to the cached original record file */
    private String cachedFileName;


    public Result(DBObject docWrapper, Status status) {
        this.docWrapper = docWrapper;
        this.status = status;
    }

    public Result(JSONObject payload, Status status) {
        this.payload = payload;
        this.status = status;
    }

    public Result(DBObject docWrapper, Status status, String errMessage) {
        this.docWrapper = docWrapper;
        this.errMessage = errMessage;
        this.status = status;
    }

    public String getErrMessage() {
        return errMessage;
    }

    public void setErrMessage(String errMessage) {
        this.errMessage = errMessage;
    }

    public Status getStatus() {
        return status;
    }

    public DBObject getDocWrapper() {
        return docWrapper;
    }

    public JSONObject getPayload() {
        return payload;
    }

    public boolean isLargeRecord() {
        return largeRecord;
    }

    public void setLargeRecord(boolean largeRecord) {
        this.largeRecord = largeRecord;
    }

    public String getCachedFileName() {
        return cachedFileName;
    }

    public void setCachedFileName(String cachedFileName) {
        this.cachedFileName = cachedFileName;
    }

    public OpMode getOpMode() {
        return opMode;
    }

    public void setOpMode(OpMode opMode) {
        this.opMode = opMode;
    }

    public enum Status {
        NONE, OK_WITH_CHANGE, OK_WITHOUT_CHANGE, ERROR
    }

    public enum OpMode {
        ADD_UPDATE, DELETE
    }
}
