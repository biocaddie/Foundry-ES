package org.neuinfo.foundry.common.model;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.Utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Created by bozyurt on 5/27/14.
 */
public class BatchInfo {
    private final String batchId;
    private int ingestedCount;
    private int submittedCount;
    private Status status = Status.NOT_STARTED;
    private Status ingestionStatus = Status.NOT_STARTED;
    private int updatedCount;
    private Date ingestionStartDatetime;
    private Date ingestionEndDatetime;
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);

    public BatchInfo(String batchId) {
        this.batchId = batchId;
    }

    public BatchInfo(String batchId, Status status) {
        this.batchId = batchId;
        this.status = status;
    }

    public String getBatchId() {
        return batchId;
    }

    public int getIngestedCount() {
        return ingestedCount;
    }

    public int getSubmittedCount() {
        return submittedCount;
    }

    public Status getStatus() {
        return status;
    }

    public void setIngestedCount(int ingestedCount) {
        this.ingestedCount = ingestedCount;
    }

    public void setSubmittedCount(int submittedCount) {
        this.submittedCount = submittedCount;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Status getIngestionStatus() {
        return ingestionStatus;
    }

    public void setIngestionStatus(Status ingestionStatus) {
        this.ingestionStatus = ingestionStatus;
    }

    public int getUpdatedCount() {
        return updatedCount;
    }

    public void setUpdatedCount(int updatedCount) {
        this.updatedCount = updatedCount;
    }

    public Date getIngestionStartDatetime() {
        return ingestionStartDatetime;
    }

    public void setIngestionStartDatetime(Date ingestionStartDatetime) {
        this.ingestionStartDatetime = ingestionStartDatetime;
    }

    public Date getIngestionEndDatetime() {
        return ingestionEndDatetime;
    }

    public void setIngestionEndDatetime(Date ingestionEndDatetime) {
        this.ingestionEndDatetime = ingestionEndDatetime;
    }

    public JSONObject toJSON() {
        JSONObject js = new JSONObject();
        js.put("batchId", batchId);
        js.put("status", status.getCode());
        js.put("ingestedCount", ingestedCount);
        js.put("submittedCount", submittedCount);
        js.put("updatedCount", updatedCount);
        js.put("ingestionStatus", ingestionStatus.getCode());
        if (ingestionStartDatetime != null) {
            js.put("ingestionStartDatetime", df.format(ingestionStartDatetime));
        }
        if (ingestionEndDatetime != null) {
            js.put("ingestionEndDatetime", df.format(ingestionEndDatetime));
        }

        return js;
    }

    public static BatchInfo fromDbObject(DBObject dbo) {
        String batchId = (String) dbo.get("batchId");
        BatchInfo bi = new BatchInfo(batchId);
        int statusCode = Utils.getIntValue(dbo.get("status"), 0);
        bi.status = Status.fromCode(statusCode);
        bi.submittedCount = Utils.getIntValue(dbo.get("submittedCount"), 0);
        bi.ingestedCount = Utils.getIntValue(dbo.get("ingestedCount"), 0);
        bi.updatedCount = Utils.getIntValue(dbo.get("updatedCount"), 0);
        int ingestionStatusCode = Utils.getIntValue(dbo.get("ingestionStatus"), 0);
        bi.ingestionStatus = Status.fromCode(ingestionStatusCode);
        bi.ingestionStartDatetime = Utils.toDate(dbo.get("ingestionStartDatetime"));
        bi.ingestionEndDatetime = Utils.toDate(dbo.get("ingestionEndDatetime"));
        return bi;
    }


}
