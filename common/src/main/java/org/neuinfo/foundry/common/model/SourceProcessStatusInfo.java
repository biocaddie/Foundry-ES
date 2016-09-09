package org.neuinfo.foundry.common.model;

import org.json.JSONObject;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by bozyurt on 1/22/16.
 */
public class SourceProcessStatusInfo {
    private String sourceID;
    private String dataSource;
    private AtomicLong finishedCount = new AtomicLong(0);
    private AtomicLong errorCount = new AtomicLong(0);
    private AtomicLong totalCount = new AtomicLong(-1);
    private AtomicInteger ingestionStatus = new AtomicInteger(NOT_STARTED);
    private AtomicInteger processingStatus = new AtomicInteger(NOT_STARTED);
    private Date startDate;
    public static final int NOT_STARTED = 0;
    public static final int RUNNING = 1;
    public static final int FINISHED = 2;


    public SourceProcessStatusInfo(String sourceID, String dataSource) {
        this.sourceID = sourceID;
        this.dataSource = dataSource;
    }

    public String getSourceID() {
        return sourceID;
    }

    public String getDataSource() {
        return dataSource;
    }

    public long getFinishedCount() {
        return finishedCount.get();
    }

    public long getErrorCount() {
        return errorCount.get();
    }

    public long getTotalCount() {
        return totalCount.get();
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount.set(totalCount);
    }

    public long incrFinishedCount() {
        return finishedCount.incrementAndGet();
    }

    public void resetFinishedCount() {
        this.finishedCount.set(0);
    }

    public long incrErrorCount() {
        return errorCount.incrementAndGet();
    }

    public void resetErrorCount() {
        errorCount.set(0);
    }

    public void setIngestionStatus(int statusCode) {
        this.ingestionStatus.set(statusCode);
    }

    public void setProcessingStatus(int statusCode) {
        this.processingStatus.set(statusCode);
    }


    public JSONObject toJSON() {
        JSONObject json = new JSONObject();
        json.put("sourceID", sourceID);
        json.put("dataSource", dataSource);
        json.put("ingestionStatus", ingestionStatus.intValue());
        json.put("processingStatus", processingStatus.intValue());
        json.put("errorCount", errorCount.intValue());
        json.put("finishedCount", finishedCount.intValue());
        json.put("totalCount", totalCount.intValue());
        return json;
    }

    public static SourceProcessStatusInfo fromJSON(JSONObject json) {
        String sourceID = json.getString("sourceID");
        String dataSource = json.getString("dataSource");
        SourceProcessStatusInfo spsi = new SourceProcessStatusInfo(sourceID, dataSource);
        int ingestionStatus = json.getInt("ingestionStatus");
        int processingStatus = json.getInt("processingStatus");
        spsi.setProcessingStatus(processingStatus);
        spsi.setIngestionStatus(ingestionStatus);
        spsi.errorCount = new AtomicLong(json.getLong("errorCount"));
        spsi.finishedCount = new AtomicLong(json.getLong("finishedCount"));
        spsi.totalCount = new AtomicLong(json.getLong("totalCount"));
        return spsi;
    }
}
