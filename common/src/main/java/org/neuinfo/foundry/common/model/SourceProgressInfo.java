package org.neuinfo.foundry.common.model;

import org.json.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Created by bozyurt on 2/6/17.
 */
public class SourceProgressInfo {
    private Date startDate;
    private Date endDate;
    private Date ingestionEndDate;
    private String sourceID;
    private String dataSource;
    private int finishedCount;
    private int ingestedCount;
    private int updatedCount;
    private int newCount;
    private int submittedCount;
    private int errorCount;
    private int ingestionStatus = NOT_STARTED;
    private int processingStatus = NOT_STARTED;
    public static final int NOT_STARTED = 0;
    public static final int RUNNING = 1;
    public static final int FINISHED = 2;

    private Map<String, Integer> stageCountMap = new HashMap<String, Integer>(11);
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);

    public SourceProgressInfo(String sourceID, String dataSource) {
        this.sourceID = sourceID;
        this.dataSource = dataSource;
    }

    public JSONObject toJSON() {
        JSONObject json = new JSONObject();
        json.put("sourceID", sourceID);
        json.put("dataSource", dataSource);
        json.put("ingestionStatus", ingestionStatus);
        json.put("processingStatus", processingStatus);
        json.put("errorCount", errorCount);
        json.put("finishedCount", finishedCount);
        json.put("submittedCount", submittedCount);
        json.put("ingestedCount", ingestedCount);
        json.put("updatedCount", updatedCount);
        json.put("newCount", newCount);
        if (startDate != null) {
            json.put("startDate", df.format(startDate));
        }
        if (endDate != null) {
            json.put("endDate", df.format(endDate));
        }
        if (ingestionEndDate != null) {
            json.put("ingestionEndDate", df.format(ingestionEndDate));
        }
        return json;
    }

    public static SourceProgressInfo fromJSON(JSONObject json) {
        String sourceID = json.getString("sourceID");
        String dataSource = json.getString("dataSource");
        SourceProgressInfo spi = new SourceProgressInfo(sourceID, dataSource);
        spi.ingestionStatus = json.getInt("ingestionStatus");
        spi.processingStatus = json.getInt("processingStatus");
        spi.errorCount = json.getInt("errorCount");
        spi.finishedCount = json.getInt("finishedCount");
        spi.ingestedCount = json.getInt("ingestedCount");
        spi.submittedCount = json.getInt("submittedCount");
        spi.updatedCount = json.getInt("updatedCount");
        spi.newCount = json.getInt("newCount");
        if (json.has("startDate")) {
            spi.startDate = parseDate(json.getString("startDate"));
        }
        if (json.has("endDate")) {
            spi.endDate = parseDate(json.getString("endDate"));
        }
        if (json.has("ingestionEndDate")) {
            spi.ingestionEndDate = parseDate(json.getString("ingestionEndDate"));
        }

        for (String key : json.keySet()) {
            if (key.endsWith("Count") && key.indexOf('_') != -1) {
                spi.stageCountMap.put(key, json.getInt(key));
            }
        }
        return spi;
    }

    public static String formatDate(Date date) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
        return df.format(date);
    }

    public static Date parseDate(String dateString) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
        Date d;
        try {
            d = df.parse(dateString);
            return d;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public Date getIngestionEndDate() {
        return ingestionEndDate;
    }

    public void setIngestionEndDate(Date ingestionEndDate) {
        this.ingestionEndDate = ingestionEndDate;
    }

    public String getSourceID() {
        return sourceID;
    }

    public void setSourceID(String sourceID) {
        this.sourceID = sourceID;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public int getFinishedCount() {
        return finishedCount;
    }

    public void setFinishedCount(int finishedCount) {
        this.finishedCount = finishedCount;
    }

    public int getIngestedCount() {
        return ingestedCount;
    }

    public void setIngestedCount(int ingestedCount) {
        this.ingestedCount = ingestedCount;
    }

    public int getUpdatedCount() {
        return updatedCount;
    }

    public void setUpdatedCount(int updatedCount) {
        this.updatedCount = updatedCount;
    }

    public int getSubmittedCount() {
        return submittedCount;
    }

    public void setSubmittedCount(int submittedCount) {
        this.submittedCount = submittedCount;
    }

    public int getErrorCount() {
        return errorCount;
    }

    public void setErrorCount(int errorCount) {
        this.errorCount = errorCount;
    }

    public int getIngestionStatus() {
        return ingestionStatus;
    }

    public void setIngestionStatus(int ingestionStatus) {
        this.ingestionStatus = ingestionStatus;
    }

    public int getProcessingStatus() {
        return processingStatus;
    }

    public void setProcessingStatus(int processingStatus) {
        this.processingStatus = processingStatus;
    }

    public int getNewCount() {
        return newCount;
    }

    public void setNewCount(int newCount) {
        this.newCount = newCount;
    }

    public Map<String, Integer> getStageCountMap() {
        return stageCountMap;
    }

}
