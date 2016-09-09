package org.neuinfo.foundry.consumers;

import com.mongodb.*;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.neuinfo.foundry.consumers.common.Configuration;
import org.neuinfo.foundry.consumers.jms.consumers.ConsumerSupport;

import javax.jms.JMSException;
import javax.jms.MessageListener;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by bozyurt on 12/10/14.
 */
public class Helper extends ConsumerSupport {

    public Helper(String queueName) {
        super(queueName);
    }

    public DBObject getDocWrapper() {
        DB db = mongoClient.getDB(super.mongoDbName);
        DBCollection collection = db.getCollection("records");
        return collection.findOne();
    }

    Configuration getConfig() {
        return this.config;
    }

    public DB getDB() {
        return mongoClient.getDB(super.mongoDbName);
    }

    public BasicDBObject getDocWrapper(String id, String collectionName) {
        DB db = mongoClient.getDB(super.mongoDbName);
        DBCollection collection = db.getCollection(collectionName);
        BasicDBObject query = new BasicDBObject("_id", new ObjectId(id));
        return (BasicDBObject) collection.findOne(query);

    }

    public void updateDates(String sourceID, String collectionName) {
        DB db = mongoClient.getDB(super.mongoDbName);
        DBCollection collection = db.getCollection(collectionName);
        DBCursor dbCursor;
        BasicDBObject query = new BasicDBObject("SourceInfo.SourceID", sourceID);
        dbCursor = collection.find(query);
        int count = 0;
        try {
            while (dbCursor.hasNext()) {
                BasicDBObject dbo = (BasicDBObject) dbCursor.next();
                BasicDBObject data = (BasicDBObject) dbo.get("Data");
                boolean changed = false;
                if (data != null) {
                    BasicDBObject transformedRec = (BasicDBObject) data.get("transformedRec");
                    if (transformedRec != null) {
                        BasicDBObject node = findBDO(transformedRec, "dataItem.releaseDate");
                        if (node != null) {
                            String oldValue = (String) node.get("releaseDate");
                            String newValue = toElasticSearchDate(oldValue, "yyyy-MM-dd");
                            if (newValue != null) {
                                node.put("releaseDate", newValue);
                                changed = true;
                            }
                        }
                        node = findBDO(transformedRec, "dataItem.depositionDate");
                        if (node != null) {
                            String oldValue = (String) node.get("depositionDate");
                            String newValue = toElasticSearchDate(oldValue, "yyyy-MM-dd");
                            if (newValue != null) {
                                node.put("depositionDate", newValue);
                                changed = true;
                            }
                        }
                    }
                }
                if (changed) {
                    JSONObject json = JSONUtils.toJSON(data, false);
                    // System.out.println(json.toString(2));
                    count++;
                    BasicDBObject updateQuery = new BasicDBObject("_id", dbo.get("_id"));
                    collection.update(updateQuery, dbo, false, false, WriteConcern.SAFE);
                    System.out.println("count:" + count);
                }
            }
        } finally {
            dbCursor.close();
        }

    }

    public static String toElasticSearchDate(String currentValue, String origFormat) {
        SimpleDateFormat origDateFormat = new SimpleDateFormat(origFormat);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        try {
            Date date = origDateFormat.parse(currentValue);
            return dateFormat.format(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    BasicDBObject findBDO(BasicDBObject transformedRec, String jsonPath) {
        String[] toks = jsonPath.split("\\.");
        BasicDBObject parent = transformedRec;
        Object child = null;
        for (String tok : toks) {
            child = parent.get(tok);
            if (child == null) {
                return null;
            }
            if (child instanceof BasicDBObject) {
                parent = (BasicDBObject) child;
            } else {
                return parent;
            }
        }
        return null;
    }

    public List<BasicDBObject> getDocWrappers(String sourceID, int limit) {
        DB db = mongoClient.getDB(super.mongoDbName);
        DBCollection collection = db.getCollection("nifRecords");
        DBCursor dbCursor;
        if (sourceID != null) {
            BasicDBObject query = new BasicDBObject("SourceInfo.SourceID", sourceID);
            if (limit > 0) {
                dbCursor = collection.find(query).limit(limit);
            } else {
                dbCursor = collection.find(query);
            }
        } else {
            if (limit > 0) {
                dbCursor = collection.find().limit(limit);
            } else {
                dbCursor = collection.find();
            }
        }
        List<BasicDBObject> docWrappers = new ArrayList<BasicDBObject>(100);
        try {
            while (dbCursor.hasNext()) {
                docWrappers.add((BasicDBObject) dbCursor.next());
            }
        } finally {
            dbCursor.close();
        }
        return docWrappers;
    }


    @Override
    public String getId() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void handleMessages(MessageListener listener) throws JMSException {
        // no op
    }
}
