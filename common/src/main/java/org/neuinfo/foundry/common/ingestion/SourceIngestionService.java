package org.neuinfo.foundry.common.ingestion;

import com.mongodb.*;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.BatchInfo;
import org.neuinfo.foundry.common.model.Source;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JSONUtils;

/**
 * Created by bozyurt on 5/27/14.
 */
public class SourceIngestionService extends BaseIngestionService {

    public void setMongoClient(MongoClient mc) {
        Assertion.assertTrue(this.mongoClient == null);
        this.mongoClient = mc;
    }

    public void setMongoDBName(String dbName) {
        this.dbName = dbName;
    }

    public void saveSource(Source source) {
        DB db = mongoClient.getDB(dbName);
        DBCollection sources = db.getCollection("sources");
        JSONObject json = source.toJSON();

        DBObject sourceDbObj = JSONUtils.encode(json, true);
        sources.insert(sourceDbObj);
    }

    public void updateSource(Source source) {
        DB db = mongoClient.getDB(dbName);
        DBCollection sources = db.getCollection("sources");
        BasicDBObject query = new BasicDBObject("sourceInformation.resourceID", source.getResourceID())
                .append("sourceInformation.dataSource", source.getDataSource());
        JSONObject json = source.toJSON();

        DBObject sourceDbObj = JSONUtils.encode(json, true);
        sources.update(query, sourceDbObj, false, false, WriteConcern.SAFE);
    }

    public void deleteSource(Source source) {
        DB db = mongoClient.getDB(dbName);
        DBCollection sources = db.getCollection("sources");
        BasicDBObject query = new BasicDBObject("sourceInformation.resourceID", source.getResourceID());
        DBObject srcDBO = sources.findOne(query);
        if (srcDBO != null) {
            sources.remove(srcDBO);
        }
    }


    public BatchInfo getBatchInfo(String nifId, String batchId) {
        DB db = mongoClient.getDB(dbName);
        DBCollection sources = db.getCollection("sources");
        BasicDBObject query = new BasicDBObject("sourceInformation.resourceID", nifId);
        BasicDBObject keys = new BasicDBObject("batchInfos", 1);
        final DBCursor cursor = sources.find(query, keys);
        BatchInfo bi = null;
        try {
            if (cursor.hasNext()) {
                DBObject dbObject = cursor.next();
                BasicDBList biList = (BasicDBList) dbObject.get("batchInfos");
                for (int i = 0; i < biList.size(); i++) {
                    final DBObject biDBO = (DBObject) biList.get(i);
                    final String aBatchId = (String) biDBO.get("batchId");
                    if (aBatchId.equals(batchId)) {
                        bi = BatchInfo.fromDbObject(biDBO);
                        break;
                    }
                }
            }
        } finally {
            cursor.close();
        }
        return bi;
    }

    public void addUpdateBatchInfo(String nifId, String dataSource, BatchInfo bi) {
        DB db = mongoClient.getDB(dbName);
        DBCollection sources = db.getCollection("sources");
        BasicDBObject query = new BasicDBObject("sourceInformation.resourceID", nifId)
                .append("sourceInformation.dataSource", dataSource);
        BasicDBObject keys = new BasicDBObject("batchInfos", 1);
        final DBCursor cursor = sources.find(query, keys);
        boolean updated = false;
        BasicDBList biList = null;
        try {
            if (cursor.hasNext()) {
                DBObject dbObject = cursor.next();
                biList = (BasicDBList) dbObject.get("batchInfos");
                for (int i = 0; i < biList.size(); i++) {
                    final DBObject biDBO = (DBObject) biList.get(i);
                    final String aBatchId = (String) biDBO.get("batchId");
                    if (aBatchId.equals(bi.getBatchId())) {
                        biDBO.put("status", bi.getStatus().getCode());
                        biDBO.put("submittedCount", bi.getSubmittedCount());
                        biDBO.put("ingestedCount", bi.getIngestedCount());
                        biDBO.put("updatedCount", bi.getUpdatedCount());
                        biDBO.put("ingestionStatus", bi.getIngestionStatus().getCode());
                        if (bi.getIngestionStartDatetime() != null) {
                            biDBO.put("ingestionStartDatetime", bi.getIngestionStartDatetime());
                        }
                        if (bi.getIngestionEndDatetime() != null) {
                            biDBO.put("ingestionEndDatetime", bi.getIngestionEndDatetime());
                        }
                        updated = true;
                        break;
                    }
                }
            }
        } finally {
            cursor.close();
        }
        //
        if (updated) {
            BasicDBObject update = new BasicDBObject("$set", new BasicDBObject("batchInfos", biList));
            sources.update(query, update);
        } else {
            final DBObject dbObject = JSONUtils.encode(bi.toJSON());
            BasicDBObject update = new BasicDBObject("$push", new BasicDBObject("batchInfos", dbObject));
            sources.update(query, update);
        }
    }

}
