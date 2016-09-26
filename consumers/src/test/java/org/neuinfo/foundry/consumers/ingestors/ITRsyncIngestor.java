package org.neuinfo.foundry.consumers.ingestors;

import com.mongodb.*;
import org.json.JSONObject;
import org.junit.Test;
import org.neuinfo.foundry.common.transform.TransformMappingUtils;
import org.neuinfo.foundry.common.transform.TransformationEngine;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.neuinfo.foundry.common.util.MongoUtils;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.common.Constants;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.RsyncIngestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by bozyurt on 5/14/15.
 */
public class ITRsyncIngestor {


    @Test
    public void testStartup() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("documentElement", "datablock");
        options.put("largeRecords", "true");
        options.put("rsyncSource", "/tmp/pdb_ftp/");
        options.put("rsyncDest", "/tmp/pdb_ftp_dest");
        options.put("filenamePattern", ".+\\.xml\\.gz$");

        RsyncIngestor ingestor = new RsyncIngestor();
        ingestor.initialize(options);
        ingestor.startup();
    }

    public void testPDBRsync() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("documentElement", "datablock");
        options.put("largeRecords", "true");
        options.put("rsyncSource", "rsync.wwpdb.org::ftp_data/structures/divided/XML-noatom/");
        options.put("rsyncDest", "/var/temp/pdb_rsync");
        options.put("port", "33444");
        options.put("filenamePattern", ".+\\.xml\\.gz$");

        RsyncIngestor ingestor = new RsyncIngestor();
        ingestor.initialize(options);
        ingestor.startup();
    }

    public void testPDBIngestionIssue() throws Exception {
        updateMongoRec();
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("documentElement", "datablock");
        options.put("largeRecords", "false");
        options.put("fullSet", "true");
        options.put("rsyncSource", "rsync.wwpdb.org::ftp_data/structures/divided/XML-noatom/");
        options.put("rsyncDest", "/var/data/foundry-es/cache/data/pdb_rsync_test");
        options.put("port", "33444");
        options.put("filenamePattern", ".+\\.xml\\.gz$");
        options.put("testMode", "true");
        options.put("primaryKeyJSONPath", "$.'PDBx:datablock'.'@datablockName'");


        String outFile = "/tmp/pdb_test.json";
        RsyncIngestor ingestor = new RsyncIngestor();
        ingestor.initialize(options);
        try {
            ingestor.startup();
            String HOME_DIR = System.getProperty("user.home");
            String transformScript = TransformMappingUtils.loadTransformMappingScript(HOME_DIR +
                    "/dev/biocaddie/data-pipeline/transformations/pdb.trs");
            TransformationEngine trEngine = new TransformationEngine(transformScript);
            if (ingestor.hasNext()) {
                Result result = ingestor.prepPayload();
                assertNotNull(result);
                assertTrue(result.getStatus() != Result.Status.ERROR);
                JSONObject payload = result.getPayload();
                JSONObject json = new JSONObject();
                json.put("originalDoc", payload);
                json.put("status", "new");
                BasicDBObject dbo = (BasicDBObject) JSONUtils.encode(json, true);
                save2Mongo(dbo);

                JSONObject transformedJson = new JSONObject();
                trEngine.transform(payload, transformedJson);
                System.out.println(transformedJson.toString(2));

                Utils.saveText(transformedJson.toString(2), outFile);
                System.out.println("saved file:" + outFile);
            }
        } finally {
            ingestor.shutdown();
        }
    }


    public static void save2Mongo(BasicDBObject dbo) {
        List<ServerAddress> mongoServers = new ArrayList<ServerAddress>(1);
        mongoServers.add(new ServerAddress("localhost", 27017));

        MongoClient mongoClient = MongoUtils.createMongoClient(mongoServers);
        DB test1 = mongoClient.getDB("test1");
        DBCollection records = test1.getCollection("records");
        records.insert(dbo, WriteConcern.SAFE);


        mongoClient.close();
    }

    public static void updateMongoRec() throws IOException {
        List<ServerAddress> mongoServers = new ArrayList<ServerAddress>(1);
        mongoServers.add(new ServerAddress("localhost", 27017));

        MongoClient mongoClient = MongoUtils.createMongoClient(mongoServers);
        DB test1 = mongoClient.getDB("test1");
        DBCollection records = test1.getCollection("records");
        BasicDBObject one = (BasicDBObject) records.findOne();

        one.put("status", "updated");
        BasicDBObject query = new BasicDBObject(Constants.MONGODB_ID_FIELD, one.get("_id"));
        Utils.saveText(JSONUtils.toJSON(one, true).toString(2), "/tmp/x.json");
        //  records.update(query, one);
    }
}
