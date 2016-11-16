package org.neuinfo.foundry.consumers.common;

import com.mongodb.*;
import org.bson.types.ObjectId;
import org.neuinfo.foundry.consumers.jms.consumers.MessagePublisher;
import org.neuinfo.foundry.consumers.plugin.IDocUpdater;

import javax.jms.JMSException;

/**
 * Created by bozyurt on 10/13/16.
 */
public abstract class GenericDocumentModifierSupport implements IDocUpdater {
    protected MongoClient mongoClient;
    protected String mongoDbName;
    protected String collectionName;
    protected String sourceID;
    protected MessagePublisher messagePublisher;
    protected String outStatus;

    public GenericDocumentModifierSupport(MongoClient mongoClient, String mongoDbName,
                                          String collectionName, String sourceID,
                                          MessagePublisher messagePublisher,
                                          String outStatus) {
        this.collectionName = collectionName;
        this.mongoDbName = mongoDbName;
        this.mongoClient = mongoClient;
        this.sourceID = sourceID;
        this.messagePublisher = messagePublisher;
        this.outStatus = outStatus;
    }


    public BasicDBObject findDocument(String primaryKey) {
        DB db = mongoClient.getDB(mongoDbName);
        DBCollection collection = db.getCollection(collectionName);
        BasicDBObject query = new BasicDBObject("primaryKey", primaryKey)
                .append("SourceInfo.SourceID", this.sourceID);
        DBCursor cursor = collection.find(query);
        BasicDBObject docDBO = null;
        try {
            if (cursor.hasNext()) {
                docDBO = (BasicDBObject) cursor.next();
            }
        } finally {
             cursor.close();
        }
        return docDBO;
    }

    public void saveDocument(BasicDBObject docDBO) {
        ObjectId oid = (ObjectId) docDBO.get("_id");
        BasicDBObject query = new BasicDBObject("_id", oid);
        DB db = mongoClient.getDB(mongoDbName);
        DBCollection collection = db.getCollection(collectionName);

        DBObject pi = (DBObject) docDBO.get("Processing");
        pi.put("status", this.outStatus);

        collection.update(query, docDBO, false, false, WriteConcern.SAFE);
    }

    public void sendMessage(String oid) {
        try {
            messagePublisher.sendMessage(oid, this.outStatus, this.collectionName);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

}
