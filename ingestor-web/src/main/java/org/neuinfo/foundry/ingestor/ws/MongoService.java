package org.neuinfo.foundry.ingestor.ws;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.mongodb.*;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.config.ServerInfo;
import org.neuinfo.foundry.common.ingestion.Configuration;
import org.neuinfo.foundry.common.ingestion.SourceIngestionService;
import org.neuinfo.foundry.common.model.*;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JSONPathProcessor;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.neuinfo.foundry.ingestor.common.ConfigLoader;

import java.net.InetAddress;
import java.util.*;

/**
 * Created by bozyurt on 7/17/14.
 */
public class MongoService {
    MongoClient mongoClient;
    String dbName;
    private static MongoService instance = null;


    public synchronized static MongoService getInstance() throws Exception {
        if (instance == null) {
            instance = new MongoService();
        }
        return instance;
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public String getDbName() {
        return dbName;
    }

    private MongoService() throws Exception {
        Configuration conf = ConfigLoader.load("ingestor-cfg.xml", false);
        this.dbName = conf.getMongoDBName();
        List<ServerAddress> servers = new ArrayList<ServerAddress>(conf.getServers().size());
        for (ServerInfo si : conf.getServers()) {
            InetAddress inetAddress = InetAddress.getByName(si.getHost());
            servers.add(new ServerAddress(inetAddress, si.getPort()));
        }
        mongoClient = new MongoClient(servers);
        mongoClient.setWriteConcern(WriteConcern.SAFE);
    }

    public synchronized void shutdown() {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }

    public List<JSONObject> getAllSources() {
        List<JSONObject> list = new LinkedList<JSONObject>();
        DB db = mongoClient.getDB(dbName);
        DBCollection sources = db.getCollection("sources");
        final DBCursor cursor = sources.find(new BasicDBObject());
        try {
            while (cursor.hasNext()) {
                BasicDBObject dbObject = (BasicDBObject) cursor.next();
                final JSONObject js = JSONUtils.toJSON(dbObject, true);
                list.add(js);
            }
        } finally {
            cursor.close();
        }
        return list;
    }

    public JSONObject getSource(String nifId) {
        DB db = mongoClient.getDB(dbName);
        DBCollection sources = db.getCollection("sources");
        BasicDBObject query = new BasicDBObject();
        query.put("sourceInformation.resourceID", nifId);
        BasicDBObject source = (BasicDBObject) sources.findOne(query);
        if (source == null) {
            return null;
        }
        return JSONUtils.toJSON(source, true);
    }

    public JSONArray findDocumentIds4Source(String nifId, Set<String> statusSet) {
        DB db = mongoClient.getDB(dbName);
        DBCollection records = db.getCollection("records");
        BasicDBObject query = new BasicDBObject("SourceInfo.SourceID", nifId);
        if (statusSet != null && !statusSet.isEmpty()) {
            List<String> statusList = new ArrayList<String>(statusSet);
            query.append("Processing.status", new BasicDBObject("$in", statusList));
        }
        BasicDBObject keys = new BasicDBObject("primaryKey", 1);
        DBCursor cursor = records.find(query, keys);
        JSONArray jsArr = new JSONArray();
        try {
            while (cursor.hasNext()) {
                BasicDBObject dbObject = (BasicDBObject) cursor.next();
                JSONObject js = JSONUtils.toJSON(dbObject, false);
                jsArr.put(js.getString("primaryKey"));
            }
        } finally {
            cursor.close();
        }
        return jsArr;
    }


    public JSONObject findDocument(String nifId, String docId) {
        DB db = mongoClient.getDB(dbName);
        DBCollection records = db.getCollection("records");
        BasicDBObject query = new BasicDBObject("primaryKey", docId).append("SourceInfo.SourceID", nifId);
        final BasicDBObject dbObject = (BasicDBObject) records.findOne(query);
        if (dbObject == null) {
            return null;
        }
        return JSONUtils.toJSON(dbObject, true);
    }

    public JSONObject findOriginalDocument(String nifId, String docId) {
        DB db = mongoClient.getDB(dbName);
        DBCollection records = db.getCollection("records");
        BasicDBObject query = new BasicDBObject("primaryKey", docId).append("SourceInfo.SourceID", nifId);
        BasicDBObject fields = new BasicDBObject("OriginalDoc", 1);
        final BasicDBObject dbObject = (BasicDBObject) records.findOne(query);
        if (dbObject == null) {
            return null;
        }
        return JSONUtils.toJSON((BasicDBObject) dbObject.get("OriginalDoc"), false);
    }

    public BasicDBObject findTheDocument(String resourceId, String docId) {
        DB db = mongoClient.getDB(dbName);
        DBCollection records = db.getCollection("records");
        BasicDBObject query = new BasicDBObject("primaryKey", docId).append("SourceInfo.SourceID", resourceId);
        return (BasicDBObject) records.findOne(query);
    }


    public boolean hasDocument(String nifId, String docId) {
        DB db = mongoClient.getDB(dbName);
        DBCollection records = db.getCollection("records");
        BasicDBObject query = new BasicDBObject("primaryKey", docId).append("SourceInfo.SourceID", nifId);
        BasicDBObject fields = new BasicDBObject("primaryKey", 1);
        return records.findOne(query, fields) != null;
    }

    public Source findSource(String nifId) {
        BasicDBObject query = new BasicDBObject("nifId", nifId);
        DB db = mongoClient.getDB(dbName);
        DBCollection sources = db.getCollection("sources");
        final DBCursor cursor = sources.find(query);

        Source source = null;
        try {
            if (cursor.hasNext()) {
                final DBObject dbObject = cursor.next();
                source = Source.fromDBObject(dbObject);
            }
        } finally {
            cursor.close();
        }
        return source;
    }


    public Organization findOrganization(String orgName, String objectId) {
        DB db = mongoClient.getDB(dbName);
        DBCollection organizations = db.getCollection("organizations");
        BasicDBObject query;
        if (objectId != null) {
            query = new BasicDBObject("_id", new ObjectId(objectId));
        } else {
            query = new BasicDBObject("name", orgName);
        }
        DBCursor cursor = organizations.find(query);
        Organization org = null;
        try {
            DBObject dbObject = cursor.next();
            org = Organization.fromDBObject(dbObject);
        } finally {
            cursor.close();
        }
        return org;
    }

    public ObjectId saveOrganization(String orgName) throws Exception {
        DB db = mongoClient.getDB(dbName);
        DBCollection organizations = db.getCollection("organizations");
        Organization org = new Organization(orgName);
        JSONObject json = org.toJSON();
        DBObject dbObject = JSONUtils.encode(json, true);
        organizations.save(dbObject);
        ObjectId id = (ObjectId) dbObject.get("_id");
        return id;
    }


    public void removeOrganization(String orgName, String objectId) throws Exception {
        DB db = mongoClient.getDB(dbName);
        DBCollection organizations = db.getCollection("organizations");
        BasicDBObject query;
        if (objectId != null) {
            query = new BasicDBObject("_id", new ObjectId(objectId));
        } else {
            query = new BasicDBObject("name", orgName);
        }

        WriteResult remove = organizations.remove(query);
        System.out.println("remove:" + remove);
    }

    public ObjectId saveUser(User user) throws Exception {
        DB db = mongoClient.getDB(dbName);
        DBCollection users = db.getCollection("users");
        JSONObject json = user.toJSON();
        DBObject dbo = JSONUtils.encode(json, true);
        users.save(dbo);
        ObjectId id = (ObjectId) dbo.get("_id");
        return id;
    }

    public User findUser(String userName, String objectId) {
        DB db = mongoClient.getDB(dbName);
        DBCollection users = db.getCollection("users");
        BasicDBObject query;
        if (objectId != null) {
            query = new BasicDBObject("_id", new ObjectId(objectId));
        } else {
            query = new BasicDBObject("username", userName);
        }
        DBCursor cursor = users.find(query);
        User user = null;
        try {
            DBObject dbObject = cursor.next();
            user = User.fromDBObject(dbObject);
        } finally {
            cursor.close();
        }
        return user;
    }

    public void removeUser(String userName, String objectId) throws Exception {
        DB db = mongoClient.getDB(dbName);
        DBCollection users = db.getCollection("users");
        BasicDBObject query;
        if (objectId != null) {
            query = new BasicDBObject("_id", new ObjectId(objectId));
        } else {
            query = new BasicDBObject("username", userName);
        }
        users.remove(query);
    }


    public void saveDocument(JSONObject payload, String batchId, String sourceId,
                             String sourceName, boolean validate,
                             Source source, String primaryKey) throws Exception {
        JsonSchema schema = null;

        if (validate && source.getSchema() != null) {
            JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
            String jsonStr = source.getSchema().toString(2);
            System.out.println(jsonStr);
            JsonNode schemaJSON = JsonLoader.fromString(jsonStr);
            schema = factory.getJsonSchema(schemaJSON);
        }

        if (schema != null && validate) {
            // validate the payload
            final JsonNode json = JsonLoader.fromString(payload.toString());
            ProcessingReport report = schema.validate(json);
            if (!report.isSuccess()) {
                throw new Exception(report.toString());
            }
        }

        if (primaryKey == null) {
            //          JSONObject pkJS = source.getPrimaryKey();
            PrimaryKeyDef pkDef = source.getPrimaryKeyDef();
            primaryKey = pkDef.prepPrimaryKey(payload);
//            primaryKey = prepPrimaryKey(payload, pkJS.getJSONArray("key"));
        }

        DocWrapper.Builder builder = new DocWrapper.Builder("new");
        final DocWrapper docWrapper = builder.batchId(batchId).payload(payload).version(1)
                .crawlDate(new Date()).sourceId(sourceId)
                .sourceName(sourceName).primaryKey(primaryKey).build();


        DB db = mongoClient.getDB(dbName);
        DBCollection records = db.getCollection("cinergiRecords");

        // now save the doc
        JSONObject json = docWrapper.toJSON();
        DBObject dbObject = JSONUtils.encode(json, true);

        records.insert(dbObject);
    }


    public static String prepPrimaryKey(JSONObject payload, JSONArray pkJSArr) throws Exception {
        StringBuilder sb = new StringBuilder();
        JSONPathProcessor processor = new JSONPathProcessor();
        int len = pkJSArr.length();
        for (int i = 0; i < len; i++) {
            String jsonPathKey = pkJSArr.getString(i);
            final List<Object> objects = processor.find(jsonPathKey, payload);
            Assertion.assertTrue(objects.size() == 1);
            final Object o = objects.get(0);
            sb.append(o.toString());
            if ((i + 1) < len) {
                sb.append("__");
            }
        }
        return sb.toString();
    }

    public void beginBatch(String nifId, String dataSource, String batchId, boolean successfulIngestion) {
        SourceIngestionService sis = new SourceIngestionService();
        sis.setMongoClient(this.mongoClient);
        sis.setMongoDBName(this.dbName);
        BatchInfo bi = new BatchInfo(batchId, Status.IN_PROCESS);
        bi.setIngestedCount(0);
        if (successfulIngestion) {
            bi.setIngestedCount(1);
        }
        bi.setSubmittedCount(1);

        sis.addUpdateBatchInfo(nifId, dataSource, bi);
    }

    public void updateBatch(String nifId, String dataSource, String batchId, boolean successfulIngestion) {
        SourceIngestionService sis = new SourceIngestionService();
        sis.setMongoClient(this.mongoClient);
        sis.setMongoDBName(this.dbName);

        final BatchInfo bi = sis.getBatchInfo(nifId, batchId);
        bi.setSubmittedCount(bi.getSubmittedCount() + 1);
        if (successfulIngestion) {
            bi.setIngestedCount(bi.getIngestedCount() + 1);
        }
        sis.addUpdateBatchInfo(nifId, dataSource, bi);
    }

    public void endBatch(String nifId, String dataSource, String batchId) {
        SourceIngestionService sis = new SourceIngestionService();
        sis.setMongoClient(this.mongoClient);
        sis.setMongoDBName(this.dbName);
        final BatchInfo bi = sis.getBatchInfo(nifId, batchId);
        bi.setStatus(Status.FINISHED);
        sis.addUpdateBatchInfo(nifId, dataSource, bi);
    }

}
