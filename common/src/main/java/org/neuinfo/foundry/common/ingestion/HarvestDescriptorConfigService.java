package org.neuinfo.foundry.common.ingestion;

import com.mongodb.*;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.IngestConfig;
import org.neuinfo.foundry.common.util.JSONUtils;

/**
 * Created by bozyurt on 8/25/15.
 */
public class HarvestDescriptorConfigService extends BaseIngestionService {

    public void saveIngestConfig(IngestConfig ic) {
        DB db = mongoClient.getDB(this.dbName);
        DBCollection ics = db.getCollection("ingestorConfigs");
        BasicDBObject query = new BasicDBObject("name", ic.getName());
        DBObject icDBO = ics.findOne(query);
        if (icDBO == null) {
            JSONObject json = ic.toJSON();
            icDBO = JSONUtils.encode(json, false);
            ics.insert(icDBO);
        } else {
            JSONObject json = ic.toJSON();
            DBObject update = JSONUtils.encode(json, false);
            query = new BasicDBObject("_id", icDBO.get("_id"));
            ics.update(query, update, false, false, WriteConcern.SAFE);
        }
    }

    public void deleteIngestConfig(IngestConfig ic) {
        DB db = mongoClient.getDB(this.dbName);
        DBCollection ics = db.getCollection("ingestorConfigs");
        BasicDBObject query = new BasicDBObject("name", ic.getName());
        DBObject icDBO = ics.findOne(query);
        if (icDBO != null) {
            ics.remove(icDBO);
        }
    }
}
