package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import org.bson.types.ObjectId;
import org.neuinfo.foundry.consumers.common.GenericDocumentModifierSupport;
import org.neuinfo.foundry.consumers.jms.consumers.MessagePublisher;

import java.util.Map;

/**
 * Created by bozyurt on 10/13/16.
 */
public class PubmedDocModifier extends GenericDocumentModifierSupport{

    public PubmedDocModifier(MongoClient mongoClient, String mongoDbName, String collectionName,
                             String sourceID, MessagePublisher messagePublisher, String outStatus) {
        super(mongoClient, mongoDbName, collectionName, sourceID, messagePublisher, outStatus);
    }

    @Override
    public void update(Map<String, String> payload) {
        String primaryKey = payload.get("primaryKey");
        String filename = payload.get("filename");
        BasicDBObject docDBO = findDocument(primaryKey);
        if (docDBO != null) {
            BasicDBObject origDocDBO = (BasicDBObject) docDBO.get("OriginalDoc");
            origDocDBO.put("Deleted", new BasicDBObject("_$", filename));
            saveDocument(docDBO);
            ObjectId oid = (ObjectId) docDBO.get("_id");
            sendMessage(oid.toString());
        }
    }
}
