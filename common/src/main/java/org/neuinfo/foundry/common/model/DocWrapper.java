package org.neuinfo.foundry.common.model;

import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.neuinfo.foundry.common.provenance.ProvenanceRec;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.openprovenance.prov.json.Converter;
import org.openprovenance.prov.xml.ProvFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by bozyurt on 5/27/14.
 */
public class DocWrapper {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    private int version;
    private Date crawlDate;
    private String crawlMethod;
    private Date indexDate;
    private String sourceId;
    private String viewId;
    private String sourceName;
    private String dataSource;
    private String status;
    private String docId;
    private JSONObject payload;
    private JSONObject provenance;
    private String batchId;
    private String primaryKey;
    private JSONObject history;
    private ObjectId origFileId;


    private DocWrapper(Builder builder) {
        this.version = builder.version;
        this.crawlDate = builder.crawlDate;
        this.crawlMethod = builder.crawlMethod;
        this.indexDate = builder.indexDate;
        this.sourceId = builder.sourceId;
        this.viewId = builder.viewId;
        this.sourceName = builder.sourceName;
        this.dataSource = builder.dataSource;
        this.status = builder.status;
        this.docId = builder.docId;
        this.payload = builder.payload;
        this.provenance = builder.provenance;
        this.batchId = builder.batchId;
        this.primaryKey = builder.primaryKey;
        this.history = builder.history;
        this.origFileId = builder.origFileId;
    }


    public JSONObject toJSON() {
        JSONObject js = new JSONObject();
        JSONObject sourceInfoJS = new JSONObject();
        JSONObject historyJS = new JSONObject();
        JSONObject dataJS = new JSONObject();
        JSONObject processingJS = new JSONObject();

        js.put("primaryKey", primaryKey);
        js.put("Version", version);
        if (crawlDate != null) {
            js.put("CrawlDate", sdf.format(crawlDate));
        }
        if (indexDate != null) {
            js.put("IndexDate", sdf.format(indexDate));
        }

        JSONUtils.add2JSON(sourceInfoJS, "SourceID", sourceId);
        JSONUtils.add2JSON(sourceInfoJS, "ViewID", viewId);
        JSONUtils.add2JSON(sourceInfoJS, "Name", sourceName);
        JSONUtils.add2JSON(sourceInfoJS, "DataSource", dataSource);

        if (history == null) {
            if (provenance != null) {
                historyJS.put("provenance", provenance);
            }
            historyJS.put("batchId", batchId);
        } else {
            historyJS = history;
        }
        processingJS.put("status", status);

        js.put("OriginalDoc", payload);
        if (origFileId != null) {
            js.put("originalFileId", origFileId.toString());
        }

        js.put("SourceInfo", sourceInfoJS);
        js.put("Data", dataJS);
        js.put("Processing", processingJS);

        js.put("History", historyJS);

        return js;
    }

    public int getVersion() {
        return version;
    }

    public Date getCrawlDate() {
        return crawlDate;
    }

    public String getCrawlMethod() {
        return crawlMethod;
    }

    public Date getIndexDate() {
        return indexDate;
    }

    public String getSourceId() {
        return sourceId;
    }

    public String getViewId() {
        return viewId;
    }

    public String getSourceName() {
        return sourceName;
    }

    public String getStatus() {
        return status;
    }

    public String getDocId() {
        return docId;
    }

    public JSONObject getPayload() {
        return payload;
    }

    public JSONObject getProvenance() {
        return provenance;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setDocId(String docId) {
        this.docId = docId;
    }

    public String getBatchId() {
        return batchId;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public JSONObject getHistory() {
        return history;
    }

    public void setHistory(JSONObject history) {
        this.history = history;
    }

    public ObjectId getOrigFileId() {
        return origFileId;
    }


    public static String formatDate(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        return sdf.format(date);
    }

    public static Date getDate(String dateStr) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        return sdf.parse(dateStr);
    }

    public static class Builder {
        private int version;
        private String docId;
        private Date crawlDate;
        private String crawlMethod;
        private Date indexDate;
        private String sourceId;
        private String viewId;
        private String sourceName;
        private String status;
        private JSONObject payload;
        private JSONObject provenance;
        private String batchId;
        private String primaryKey;
        private String dataSource;
        private JSONObject history;
        private ObjectId origFileId;

        public Builder(String status) {
            this.status = status;
        }

        public Builder version(int version) {
            this.version = version;
            return this;
        }

        public Builder origFileId(ObjectId origFileId) {
            this.origFileId = origFileId;
            return this;
        }

        public Builder crawlDate(Date crawlDate) {
            this.crawlDate = crawlDate;
            return this;
        }

        public Builder indexDate(Date indexDate) {
            this.indexDate = indexDate;
            return this;
        }

        public Builder crawlMethod(String crawlMethod) {
            this.crawlMethod = crawlMethod;
            return this;
        }

        public Builder sourceId(String sourceId) {
            this.sourceId = sourceId;
            return this;
        }

        public Builder viewId(String viewId) {
            this.viewId = viewId;
            return this;
        }

        public Builder sourceName(String sourceName) {
            this.sourceName = sourceName;
            return this;
        }

        public Builder dataSource(String dataSource) {
            this.dataSource = dataSource;
            return this;
        }

        public Builder docId(String docId) {
            this.docId = docId;
            return this;
        }

        public Builder payload(JSONObject payload) {
            this.payload = payload;
            return this;
        }

        public Builder batchId(String batchId) {
            this.batchId = batchId;
            return this;
        }

        public Builder primaryKey(String primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        public Builder primaryKey(JSONObject history) {
            this.history = history;
            return this;
        }


        public Builder provenance(ProvenanceRec provRec) throws IOException {
            org.openprovenance.prov.model.ProvFactory pFactory = new ProvFactory();
            final Converter convert = new Converter(pFactory);
            final String jsStr = convert.getString(provRec.getDoc());
            JSONObject js = new JSONObject(jsStr);
            this.provenance = js;
            return this;
        }

        public DocWrapper build() {
            return new DocWrapper(this);
        }

    }
}
