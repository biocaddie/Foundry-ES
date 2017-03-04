package org.neuinfo.foundry.jms.common;

import com.mongodb.*;
import org.neuinfo.foundry.common.model.BatchInfo;
import org.neuinfo.foundry.common.model.Source;
import org.neuinfo.foundry.common.model.SourceProcessStatusInfo;
import org.neuinfo.foundry.common.model.Status;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.neuinfo.foundry.common.util.Utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by bozyurt on 1/22/16.
 */
public class SourceProcessStatusManager {
    MongoClient mongoClient;
    String dbName;
    Map<String, SourceProcessStatusInfo> spsiMap = new ConcurrentHashMap<String, SourceProcessStatusInfo>();
    StatusUpdateSaver sus;
    RecoveryManager recoveryManager;

    public SourceProcessStatusManager(MongoClient mongoClient, String dbName) {
        this.mongoClient = mongoClient;
        this.dbName = dbName;
        DB db = mongoClient.getDB(this.dbName);
        DBCollection collection = db.getCollection("sources");
        DBCursor cursor = collection.find(new BasicDBObject());
        try {
            while (cursor.hasNext()) {
                DBObject dbo = cursor.next();
                Source source = Source.fromDBObject(dbo);
                String key = prepKey(source.getResourceID(), source.getDataSource());
                SourceProcessStatusInfo spsi = getSourceProcessStatusInfo(source);
                spsiMap.put(key, spsi);
            }
        } finally {
            cursor.close();
        }
        this.recoveryManager = RecoveryManager.getInstance();
        this.sus = new StatusUpdateSaver(this, this.recoveryManager);
        Timer timer = new Timer(true);
        // every 5 seconds save processing progress if any to
        timer.schedule(this.sus, 0L, 5 * 1000L);
    }

    SourceProcessStatusInfo getSourceProcessStatusInfo(Source source) {
        String processID = Utils.prepBatchId(new Date());
        SourceProcessStatusInfo spsi = new SourceProcessStatusInfo(source.getResourceID(), source.getDataSource(),
                processID);
        BatchInfo bi = source.getLatestBatchInfo();
        if (bi != null) {
            spsi.setIngestionStatus(bi.getIngestionStatus().getCode());
            spsi.setProcessingStatus(bi.getStatus().getCode());
        } else {
            spsi.setIngestionStatus(Status.NOT_STARTED.getCode());
            spsi.setProcessingStatus(Status.NOT_STARTED.getCode());
        }
        return spsi;
    }
    public void resetSource(String resourceID, String dataSource) {
        String key = prepKey(resourceID, dataSource);
        SourceProcessStatusInfo spsi = spsiMap.get(key);
        if (spsi != null) {
            spsi.resetFinishedCount();
            DB db = mongoClient.getDB(this.dbName);
            DBCollection collection = db.getCollection("source_prog_infos");
            BasicDBObject query = new BasicDBObject("sourceID", spsi.getSourceID())
                    .append("dataSource", spsi.getDataSource());
            BasicDBObject existingDBO = (BasicDBObject) collection.findOne(query);
            if (existingDBO != null) {
                spsi = SourceProcessStatusInfo.fromJSON( JSONUtils.toJSON(existingDBO, false));
                spsiMap.put(key, spsi);
            }

        }

    }
    public void updateStatus(String status, DBObject theDoc) {
        DBObject siDBO = (DBObject) theDoc.get("SourceInfo");
        String resourceID = (String) siDBO.get("SourceID");
        String dataSource = (String) siDBO.get("DataSource");
        String key = prepKey(resourceID, dataSource);
        SourceProcessStatusInfo spsi = spsiMap.get(key);
        if (spsi == null) {
            DB db = mongoClient.getDB(this.dbName);
            DBCollection collection = db.getCollection("sources");
            BasicDBObject query = new BasicDBObject("sourceInformation.resourceID", resourceID)
                    .append("sourceInformation.dataSource", dataSource);
            DBObject sourceDBO = collection.findOne(query);

            Source source = Source.fromDBObject(sourceDBO);
            spsi = getSourceProcessStatusInfo(source);
            spsiMap.put(key, spsi);
        }
        Assertion.assertNotNull(spsi);
        if (spsi.getStartDate() == null) {
            spsi.setStartDate(new Date());
        }
        if (status.equals("finished")) {
            spsi.incrFinishedCount();
            System.out.println(">>>>>>>>>>>>>>>>>>> " + spsi.getFinishedCount());
        } else if (status.equals("error")) {
            spsi.incrErrorCount();
        }
    }

    void saveStatus(SourceProcessStatusInfo spsi) {
        DB db = mongoClient.getDB(this.dbName);
        DBCollection collection = db.getCollection("source_prog_infos");
        BasicDBObject query = new BasicDBObject("sourceID", spsi.getSourceID())
                .append("dataSource", spsi.getDataSource());
        DBCursor cursor = collection.find(query);
        BasicDBObject existingDBO = null;

        try {
            if (cursor.hasNext()) {
                existingDBO = (BasicDBObject) cursor.next();
            }
        } finally {
            cursor.close();
        }

        if (existingDBO != null) {
            System.out.println("++++++++++++++++>> saving status" + spsi);
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);
            spsi.setIngestedCount(existingDBO.getLong("ingestedCount"));
            spsi.setIngestionEndDate(
                    SourceProcessStatusInfo.parseDate(existingDBO.getString("ingestionEndDate"), df));
            spsi.setIngestionStatus(existingDBO.getInt("ingestionStatus"));
            spsi.setProcessingStatus(existingDBO.getInt("processingStatus"));
            // before saving see if the process is finished
            spsi.checkIfFinished();
            BasicDBObject query2 = new BasicDBObject("_id", existingDBO.getObjectId("_id"));
            BasicDBObject setValues = new BasicDBObject("errorCount", spsi.getErrorCount())
                    .append("totalCount", spsi.getTotalCount())
                    .append("finishedCount", spsi.getFinishedCount())
                    .append("processingStatus", spsi.getProcessingStatus());
            if (spsi.getEndDate() != null) {
                setValues.append("endDate", df.format(spsi.getEndDate()));
            }
            collection.update(query2, new BasicDBObject("$set", setValues));
        }

    }

    public void shutdown() {
        // save counters
        for (String key : spsiMap.keySet()) {
            SourceProcessStatusInfo spsi = spsiMap.get(key);
            recoveryManager.put(key, spsi);
        }
        if (recoveryManager != null) {
            recoveryManager.shutdown();
        }
    }

    public static String prepKey(String resourceID, String dataSource) {
        StringBuilder sb = new StringBuilder();
        sb.append(resourceID).append(':').append(dataSource);
        return sb.toString();
    }



    public static class StatusUpdateSaver extends TimerTask {
        SourceProcessStatusManager spsMan;
        RecoveryManager recoveryManager;
        Map<String, SourceProcessStatusInfo> spsiPrevMap = new HashMap<String, SourceProcessStatusInfo>();

        public StatusUpdateSaver(SourceProcessStatusManager spsMan, RecoveryManager recoveryManager) {
            this.spsMan = spsMan;
            this.recoveryManager = recoveryManager;
        }

        @Override
        public void run() {
            for (String key : spsMan.spsiMap.keySet()) {
                SourceProcessStatusInfo spsi = spsMan.spsiMap.get(key);
                SourceProcessStatusInfo prevSPSI = spsiPrevMap.get(key);
                if (prevSPSI == null) {
                    recoveryManager.put(key, spsi);
                    spsMan.saveStatus(spsi);
                    prevSPSI = new SourceProcessStatusInfo(spsi);
                    spsiPrevMap.put(key, prevSPSI);
                } else {
                    if (prevSPSI.getFinishedCount() != spsi.getFinishedCount()
                            || prevSPSI.getErrorCount() != spsi.getErrorCount()) {
                        recoveryManager.put(key, spsi);
                        spsMan.saveStatus(spsi);
                        prevSPSI = new SourceProcessStatusInfo(spsi);
                        spsiPrevMap.put(key, prevSPSI);
                    }
                }
            }
        }
    }
}
