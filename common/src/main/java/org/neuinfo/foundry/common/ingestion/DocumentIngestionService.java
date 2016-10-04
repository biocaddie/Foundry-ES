package org.neuinfo.foundry.common.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.mongodb.*;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.config.IMongoConfig;
import org.neuinfo.foundry.common.model.*;
import org.neuinfo.foundry.common.util.*;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by bozyurt on 5/27/14.
 */
public class DocumentIngestionService extends BaseIngestionService {
    private Source source;
    private JsonSchema schema;
    private GridFSService gridFSService;
    private final static Logger logger = Logger.getLogger(DocumentIngestionService.class);

    @Override
    public void start(IMongoConfig conf) throws UnknownHostException {
        super.start(conf);
        gridFSService = new GridFSService();
        gridFSService.start(conf);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        if (gridFSService != null) {
            gridFSService.shutdown();
        }
    }

    /**
     * to be used for web services, other use <code>startup()</code>,
     * <code>shutdown()</code> lifecycle methods
     *
     * @param dbName
     * @param mongoClient
     */
    public void initialize(String dbName,
                           MongoClient mongoClient) {
        this.dbName = dbName;
        this.mongoClient = mongoClient;
    }

    public void beginBatch(Source source, String batchId) {
        SourceIngestionService sis = new SourceIngestionService();
        sis.setMongoClient(this.mongoClient);
        sis.setMongoDBName(this.dbName);
        BatchInfo bi = new BatchInfo(batchId, Status.IN_PROCESS);
        bi.setIngestedCount(0);
        bi.setSubmittedCount(0);
        bi.setUpdatedCount(0);
        bi.setIngestionStatus(Status.IN_PROCESS);
        bi.setIngestionStartDatetime(new Date());
        sis.addUpdateBatchInfo(source.getResourceID(), source.getDataSource(), bi);
    }



    public void endBatch(Source source, String batchId,
                         int ingestedCount, int submittedCount, int updatedCount) {
        SourceIngestionService sis = new SourceIngestionService();
        sis.setMongoClient(this.mongoClient);
        sis.setMongoDBName(this.dbName);
        BatchInfo bi = new BatchInfo(batchId, Status.FINISHED);
        bi.setSubmittedCount(submittedCount);
        bi.setIngestedCount(ingestedCount);
        bi.setUpdatedCount(updatedCount);
        bi.setIngestionStatus(Status.FINISHED);
        bi.setIngestionEndDatetime(new Date());
        sis.addUpdateBatchInfo(source.getResourceID(), source.getDataSource(), bi);
    }

    public Source findSource(String nifId, String dataSource) {
        BasicDBObject query = new BasicDBObject("sourceInformation.resourceID", nifId);
        if (dataSource != null) {
            query.append("sourceInformation.dataSource", dataSource);
        }
        DB db = mongoClient.getDB(dbName);
        DBCollection sources = db.getCollection("sources");
        return MongoUtils.getSource(query, sources);
    }


    public List<Source> findSourcesWithTransformScript() {
        BasicDBObject query = new BasicDBObject("transformationInfo.transformScript",
                new BasicDBObject("$exists", true));
        DB db = mongoClient.getDB(dbName);
        DBCollection sources = db.getCollection("sources");
        final DBCursor cursor = sources.find(query);
        List<Source> sourceList = new ArrayList<Source>();
        try {
            while (cursor.hasNext()) {
                final DBObject dbObject = cursor.next();
                Source source = Source.fromDBObject(dbObject);
                if (source.getTransformScript() != null && !source.getTransformScript().isEmpty()) {
                    sourceList.add(source);
                }
            }
        } finally {
            cursor.close();
        }
        return sourceList;
    }

    public void deleteDocuments4Resource(String collectionName, String resourceId, String dataSource) {
        BasicDBObject query = new BasicDBObject("SourceInfo.SourceID", resourceId);

        if (dataSource != null) {
            query.append("SourceInfo.DataSource", dataSource);
        }

        DB db = mongoClient.getDB(dbName);
        BasicDBObject query2 = (BasicDBObject) query.copy();
        DBCollection records = db.getCollection(collectionName);
        DBCursor cursor = records.find(query2.append("originalFileId", new BasicDBObject("$exists", true)),
                new BasicDBObject("originalFileId", 1));
        List<String> originalFileIdList = new LinkedList<String>();
        try {
            while (cursor.hasNext()) {
                final DBObject dbObject = cursor.next();
                String originalFileId = (String) dbObject.get("originalFileId");
                if (this.gridFSService != null && !Utils.isEmpty(originalFileId)) {
                    originalFileIdList.add(originalFileId);
                }
            }
        } finally {
            cursor.close();
        }
        int i = 0;
        for (String originalFileId : originalFileIdList) {
            this.gridFSService.deleteJSONFile(new ObjectId(originalFileId));
            i++;
            if ((i % 100) == 0) {
                logger.info(String.format("Deleted %d out of %d from GridFS", i, originalFileIdList.size()));
            }
        }
        originalFileIdList = null;

        long count = records.count(query);
        System.out.println("found " + count + " records.");
        System.out.println("deleting...");
        records.remove(query);
    }

    public void cleanupGridFSDuplicates(String collectionName, String resourceId, String dataSource) {
        BasicDBObject query = new BasicDBObject("SourceInfo.SourceID", resourceId);
        if (dataSource != null) {
            query.append("SourceInfo.DataSource", dataSource);
        }
        DB db = mongoClient.getDB(dbName);
        BasicDBObject keys = new BasicDBObject("originalFileId", 1).append("filename", 1);
        DBCollection records = db.getCollection(collectionName);
        DBCursor cursor = records.find(query.append("originalFileId", new BasicDBObject("$exists", true)),
                keys);
        Map<String, String> id2FilenameMap = new HashMap<String, String>();

        try {
            while (cursor.hasNext()) {
                final DBObject dbObject = cursor.next();
                String originalFileId = (String) dbObject.get("originalFileId");
                if (this.gridFSService != null && !Utils.isEmpty(originalFileId)) {
                    String filename = (String) dbObject.get("filename");
                    id2FilenameMap.put(originalFileId, filename);
                }
            }
        } finally {
            cursor.close();
        }
        if (this.gridFSService != null) {
            System.out.println("id2FilenameMap.size:" + id2FilenameMap.size());
            for (Map.Entry<String, String> entry : id2FilenameMap.entrySet()) {
                String id = entry.getKey();
                String filename = entry.getValue();
                gridFSService.deleteDuplicates(new ObjectId(id), filename);
            }
        }
    }

    public void removeDocument(DBObject document, String collectionName) {
        BasicDBObject query = new BasicDBObject();
        query.put("_id", document.get("_id"));
        DB db = mongoClient.getDB(dbName);
        DBCollection records = db.getCollection(collectionName);
        records.remove(query);
    }

    public List<BasicDBObject> findDocuments(String sourceId, String dataSource,
                                             String collectionName, List<String> fields) {
        DB db = mongoClient.getDB(dbName);
        DBCollection collection = db.getCollection(collectionName);
        BasicDBObject query = new BasicDBObject("SourceInfo.SourceID", sourceId);
        if (dataSource != null) {
            query.append("SourceInfo.DataSource", dataSource);
        }
        BasicDBObject projection = new BasicDBObject();
        /*
        for(String field: fields) {
            projection.append(field,1);
        } */
        projection.append("OriginalDoc", 1);
        DBCursor cursor = collection.find(query, projection);
        List<BasicDBObject> docList = new LinkedList<BasicDBObject>();
        try {
            while (cursor.hasNext()) {
                docList.add((BasicDBObject) cursor.next());
            }

        } finally {
            cursor.close();
        }
        return docList;
    }

    public BasicDBObject findDocumentByPK(String pkValue, Source theSource, String collectionName) {
        DB db = mongoClient.getDB(dbName);
        DBCollection collection = db.getCollection(collectionName);
        BasicDBObject query = new BasicDBObject("primaryKey", pkValue)
                .append("SourceInfo.SourceID", theSource.getResourceID())
                .append("SourceInfo.DataSource", theSource.getDataSource());
        DBCursor cursor = collection.find(query);
        BasicDBObject docWrapper = null;
        try {
            if (cursor.hasNext()) {
                docWrapper = (BasicDBObject) cursor.next();
            }
        } finally {
            cursor.close();
        }
        return docWrapper;
    }

    public BasicDBObject findDocument(JSONObject payload, String collectionName) throws Exception {
        DB db = mongoClient.getDB(dbName);
        DBCollection collection = db.getCollection(collectionName);
        PrimaryKeyDef pkDef = source.getPrimaryKeyDef();

        String primaryKey = pkDef.prepPrimaryKey(payload);
        BasicDBObject query = new BasicDBObject("primaryKey", primaryKey)
                .append("SourceInfo.SourceID", source.getResourceID())
                .append("SourceInfo.DataSource", source.getDataSource());
        DBCursor cursor = collection.find(query);

        BasicDBObject docWrapper = null;
        try {
            if (cursor.hasNext()) {
                docWrapper = (BasicDBObject) cursor.next();
            }
        } finally {
            cursor.close();
        }
        return docWrapper;
    }

    public void setSource(Source source) throws IOException, ProcessingException {
        this.source = source;
        if (source.getSchema() != null) {
            JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
            String jsonStr = source.getSchema().toString(2);
            System.out.println(jsonStr);
            JsonNode schemaJSON = JsonLoader.fromString(jsonStr);
            this.schema = factory.getJsonSchema(schemaJSON);
        }
    }

    public DocWrapper saveDocument(JSONObject payload, String batchId, Source source,
                                   String outStatus, String collectionName) throws Exception {
        return saveDocument(payload, batchId, source, outStatus, collectionName, true);
    }


    public void updateDocument(BasicDBObject docWrapper, String collectionName, String batchId) {
        ObjectId oid = (ObjectId) docWrapper.get("_id");
        BasicDBObject query = new BasicDBObject("_id", oid);

        DBObject historyDBO = (DBObject) docWrapper.get("History");
        historyDBO.put("batchId", batchId);
        DB db = mongoClient.getDB(dbName);
        DBCollection collection = db.getCollection(collectionName);

        collection.update(query, docWrapper, false, false, WriteConcern.SAFE);
    }

    public DocWrapper prepareDocWrapper(JSONObject payload, ObjectId originalFileId, String batchId,
                                        Source source, String outStatus) throws Exception {
        PrimaryKeyDef pkDef = source.getPrimaryKeyDef();
        String primaryKey = pkDef.prepPrimaryKey(payload);

        DocWrapper.Builder builder = new DocWrapper.Builder(outStatus); // "new"
        final DocWrapper docWrapper = builder.batchId(batchId).payload(payload).version(1)
                .crawlDate(new Date()).sourceId(source.getResourceID())
                .sourceName(source.getName()).primaryKey(primaryKey)
                .dataSource(source.getDataSource()).origFileId(originalFileId).build();
        return docWrapper;
    }

    public ObjectId saveDocument(DocWrapper docWrapper, String collectionName) throws Exception {
        DB db = mongoClient.getDB(dbName);
        DBCollection collection = db.getCollection(collectionName); // "records");

        // now save the doc
        JSONObject json = docWrapper.toJSON();
        DBObject dbObject = JSONUtils.encode(json, true);

        collection.insert(dbObject, WriteConcern.SAFE);
        return (ObjectId) dbObject.get("_id");
    }


    public DocWrapper saveDocument(JSONObject payload, String batchId, Source source, String outStatus,
                                   String collectionName, boolean validate) throws Exception {
        if (schema != null && validate) {
            // validate the payload
            final JsonNode json = JsonLoader.fromString(payload.toString());
            ProcessingReport report = schema.validate(json);
            if (!report.isSuccess()) {
                throw new Exception(report.toString());
            }
        }

        PrimaryKeyDef pkDef = source.getPrimaryKeyDef();
        String primaryKey = pkDef.prepPrimaryKey(payload);

        DocWrapper.Builder builder = new DocWrapper.Builder(outStatus); // "new"
        final DocWrapper docWrapper = builder.batchId(batchId).payload(payload).version(1)
                .crawlDate(new Date()).sourceId(source.getResourceID())
                .sourceName(source.getName()).primaryKey(primaryKey)
                .dataSource(source.getDataSource()).build();


        DB db = mongoClient.getDB(dbName);
        DBCollection collection = db.getCollection(collectionName); // "records");

        // now save the doc
        JSONObject json = docWrapper.toJSON();
        DBObject dbObject = JSONUtils.encode(json, true);

        collection.insert(dbObject, WriteConcern.SAFE);
        return docWrapper;
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
}
