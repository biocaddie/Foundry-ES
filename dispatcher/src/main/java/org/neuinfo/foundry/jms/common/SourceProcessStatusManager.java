package org.neuinfo.foundry.jms.common;

import com.mongodb.*;
import org.neuinfo.foundry.common.model.BatchInfo;
import org.neuinfo.foundry.common.model.Source;
import org.neuinfo.foundry.common.model.SourceProcessStatusInfo;
import org.neuinfo.foundry.common.model.Status;
import org.neuinfo.foundry.common.util.Assertion;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by bozyurt on 1/22/16.
 */
public class SourceProcessStatusManager {
    MongoClient mongoClient;
    String dbName;
    Map<String, SourceProcessStatusInfo> spsiMap = new ConcurrentHashMap<String, SourceProcessStatusInfo>();

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
    }

    SourceProcessStatusInfo getSourceProcessStatusInfo(Source source) {
        SourceProcessStatusInfo spsi = new SourceProcessStatusInfo(source.getRepositoryID(),source.getDataSource());
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

    public void updateStatus(String status, DBObject theDoc) {
        // String collectionName = (String) theDoc.get("collectionName");
        DBObject siDBO = (DBObject) theDoc.get("SourceInfo");
        String resourceID = (String) siDBO.get("SourceID");
        String dataSource = (String) siDBO.get("DataSource");
        String key = prepKey(resourceID, dataSource);
        SourceProcessStatusInfo spsi = spsiMap.get(key);
        if (spsi == null ) {
            DB db = mongoClient.getDB(this.dbName);
            DBCollection collection = db.getCollection("sources");
            BasicDBObject query = new BasicDBObject("sourceInformation.resourceID", resourceID)
                .append("sourceInformation.dataSource", dataSource);
            DBObject sourceDBO =  collection.findOne(query);

            Source source = Source.fromDBObject(sourceDBO);
            spsi = getSourceProcessStatusInfo(source);
            spsiMap.put(key, spsi);
        }
        Assertion.assertNotNull(spsi);
        if (status.equals("finished")) {
            spsi.incrFinishedCount();
        } else if (status.equals("error")) {
            spsi.incrErrorCount();
        }
    }

    public static String prepKey(String resourceID, String dataSource) {
        StringBuilder sb = new StringBuilder();
        sb.append(resourceID).append(':').append(dataSource);
        return sb.toString();
    }
}
