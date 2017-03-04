package org.neuinfo.foundry.common.model;

import org.json.JSONObject;
import org.neuinfo.foundry.common.util.Utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private AtomicLong ingestedCount = new AtomicLong(0);
    private AtomicLong submittedCount = new AtomicLong(0);
    private AtomicLong updatedCount = new AtomicLong(0);

    private AtomicInteger ingestionStatus = new AtomicInteger(NOT_STARTED);
    private AtomicInteger processingStatus = new AtomicInteger(NOT_STARTED);
    private Date startDate;
    private Date endDate;
    private Date ingestionEndDate;
    private String processID;
    private AtomicBoolean changed = new AtomicBoolean(false);
    public static final int NOT_STARTED = 0;
    public static final int RUNNING = 1;
    public static final int FINISHED = 2;
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);

    public SourceProcessStatusInfo(String sourceID, String dataSource, String processID) {
        this.sourceID = sourceID;
        this.dataSource = dataSource;
        this.processID = processID;
    }

    public SourceProcessStatusInfo(SourceProcessStatusInfo other) {
        this.sourceID = other.sourceID;
        this.dataSource = other.dataSource;
        this.processID = other.processID;
        this.finishedCount = new AtomicLong(other.finishedCount.get());
        this.errorCount = new AtomicLong(other.errorCount.get());
        this.totalCount = new AtomicLong(other.totalCount.get());
        this.ingestionStatus = new AtomicInteger(other.ingestionStatus.get());
        this.processingStatus = new AtomicInteger(other.processingStatus.get());
        this.startDate = other.startDate != null ? new Date(other.startDate.getTime()) : null;
        this.endDate = other.endDate != null ? new Date(other.endDate.getTime()) : null;
        this.changed = new AtomicBoolean(other.changed.get());
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

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getIngestionEndDate() {
        return ingestionEndDate;
    }

    public void setIngestionEndDate(Date ingestionEndDate) {
        this.ingestionEndDate = ingestionEndDate;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount.set(totalCount);
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public long incrFinishedCount() {
        long finishCount = finishedCount.incrementAndGet();
        changed.compareAndSet(true, true);
        return finishCount;
    }

    public void resetChangedState() {
        changed.set(false);
    }

    public void resetFinishedCount() {
        this.finishedCount.set(0);
    }

    public long incrErrorCount() {
        long errCount = errorCount.incrementAndGet();
        changed.compareAndSet(true, true);
        return errCount;
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

    public String getProcessID() {
        return processID;
    }

    public long getIngestedCount() {
        return ingestedCount.get();
    }

    public void setIngestedCount(long ingestedCount) {
        this.ingestedCount.set(ingestedCount);
    }

    public long getSubmittedCount() {
        return submittedCount.get();
    }

    public void setSubmittedCount(long submittedCount) {
        this.submittedCount.set(submittedCount);
    }

    public long getUpdatedCount() {
        return updatedCount.get();
    }

    public void setUpdatedCount(AtomicLong updatedCount) {
        this.updatedCount = updatedCount;
    }

    public int getProcessingStatus() {
        return processingStatus.get();
    }

    public void checkIfFinished() {
        if (getIngestedCount() > 0 && (getFinishedCount() > 0 || getErrorCount() > 0)) {
            long total = getFinishedCount() + getErrorCount();
            if (getIngestedCount() <= total) {
                setEndDate(new Date());
                setProcessingStatus(FINISHED);
            } else {
                long diff = getIngestedCount() - getUpdatedCount();
            }
        }
    }

    public JSONObject toJSON() {
        JSONObject json = new JSONObject();
        json.put("sourceID", sourceID);
        json.put("dataSource", dataSource);
        json.put("processID", processID);
        json.put("ingestionStatus", ingestionStatus.get());
        json.put("processingStatus", processingStatus.get());
        json.put("errorCount", errorCount.get());
        json.put("finishedCount", finishedCount.get());
        json.put("totalCount", totalCount.get());
        json.put("submittedCount", submittedCount.get());
        json.put("ingestedCount", ingestedCount.get());
        json.put("updatedCount", updatedCount.get());
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

    public static SourceProcessStatusInfo fromJSON(JSONObject json) {
        String sourceID = json.getString("sourceID");
        String dataSource = json.getString("dataSource");
        String processID = json.getString("processID");
        SourceProcessStatusInfo spsi = new SourceProcessStatusInfo(sourceID, dataSource, processID);
        int ingestionStatus = json.getInt("ingestionStatus");
        int processingStatus = json.getInt("processingStatus");
        spsi.setProcessingStatus(processingStatus);
        spsi.setIngestionStatus(ingestionStatus);
        spsi.errorCount = new AtomicLong(json.getLong("errorCount"));
        spsi.finishedCount = new AtomicLong(json.getLong("finishedCount"));
        spsi.totalCount = new AtomicLong(json.getLong("totalCount"));
        spsi.ingestedCount = new AtomicLong(json.getLong("ingestedCount"));
        spsi.updatedCount = new AtomicLong(json.getLong("updatedCount"));
        spsi.submittedCount = new AtomicLong(json.getLong("submittedCount"));
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
        if (json.has("startDate")) {
            spsi.startDate = parseDate(json.getString("startDate"), df);
        }
        if (json.has("endDate")) {
            spsi.endDate = parseDate(json.getString("endDate"), df);
        }
        if (json.has("ingestionEndDate")) {
            spsi.ingestionEndDate = parseDate(json.getString("ingestionEndDate"), df);
        }
        return spsi;
    }



    public static Date parseDate(String dateString, DateFormat df) {
        Date d;
        try {
            d = df.parse(dateString);
            return d;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SourceProcessStatusInfo{");
        sb.append("sourceID='").append(sourceID).append('\'');
        sb.append(", dataSource='").append(dataSource).append('\'');
        sb.append(", finishedCount=").append(finishedCount);
        sb.append(", errorCount=").append(errorCount);
        sb.append(", totalCount=").append(totalCount);
        sb.append(", ingestedCount=").append(ingestedCount);
        sb.append(", submittedCount=").append(submittedCount);
        sb.append(", updatedCount=").append(updatedCount);
        sb.append(", ingestionStatus=").append(ingestionStatus);
        sb.append(", processingStatus=").append(processingStatus);
        sb.append(", startDate=").append(startDate);
        sb.append(", endDate=").append(endDate);
        sb.append(", ingestionEndDate=").append(ingestionEndDate);
        sb.append(", processID='").append(processID).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
