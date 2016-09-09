package org.neuinfo.foundry.consumers.jms.consumers;

import com.mongodb.*;
import org.json.JSONObject;
import org.neuinfo.foundry.common.ingestion.SourceIngestionService;
import org.neuinfo.foundry.common.model.BatchInfo;
import org.neuinfo.foundry.common.model.Status;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JSONUtils;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;

/**
 * Created by bozyurt on 5/28/14.
 */
public class IndexCheckpointConsumer extends JMSConsumerSupport implements MessageListener {

    public IndexCheckpointConsumer(String queueName) {
        super(queueName);
    }

    private void handle(String batchId, String sourceId) {
        SourceIngestionService sis = new SourceIngestionService();
        try {
            sis.setMongoClient(super.mongoClient);
            sis.setMongoDBName(super.mongoDbName);
            // MongoDbHelper helper = new MongoDbHelper(super.mongoDbName, super.mongoClient);

            BatchInfo batchInfo = sis.getBatchInfo(sourceId, batchId);  //helper.getBatchInfo(sourceId, batchId);
            Assertion.assertNotNull(batchInfo);
            // poll every second till batch insertion is finished
            while (batchInfo.getStatus() != Status.FINISHED) {
                synchronized (this) {
                    try {
                        this.wait(1000L);
                    } catch (InterruptedException e) {
                        // no op
                    }
                }
                batchInfo = sis.getBatchInfo(sourceId, batchId);
            }
            // get the count of the index_cp and error status documents for batchId/sourceId combination
            // if the sum equals batchInfo.ingestedCount then index_cp -> index
            DB db = mongoClient.getDB(super.mongoDbName);
            DBCollection records = db.getCollection("records");
            BasicDBObject query = new BasicDBObject("SourceInfo.SourceID", sourceId).append("History.batchId", batchId);
            BasicDBObject keys = new BasicDBObject("Processing", 1);

            int ready2IndexCount = 0;
            int erroredCount = 0;
            final DBCursor cursor = records.find(query, keys);
            try {
                while (cursor.hasNext()) {
                    final DBObject dbObject = cursor.next();

                    String docStatus = (String) JSONUtils.findNested(dbObject, "Processing.status");
                    if (docStatus.equals("index_cp")) {
                        ready2IndexCount++;
                    } else if (docStatus.equals("error")) {
                        erroredCount++;
                    }
                }
            } finally {
                cursor.close();
            }

            if (batchInfo.getIngestedCount() == (ready2IndexCount + erroredCount)) {
                BasicDBObject update = new BasicDBObject("$set",
                        new BasicDBObject("Processing.status", "index"));

                records.update(query, update, false, true);
            }
        } finally {
            if (sis != null) {
                sis.shutdown();
            }
        }
    }

    @Override
    public void onMessage(Message message) {
        try {
            ObjectMessage om = (ObjectMessage) message;
            String payload = (String) om.getObject();
            System.out.println("payload:" + payload);
            JSONObject json = new JSONObject(payload);
            String sourceId = json.getString("SourceID");
            String batchId = json.getString("batchId");
            String objectId = json.getString("oid");
            System.out.format("objectId:%s sourceId:%s batchId:%s%n", objectId, sourceId, batchId);
            handle(batchId, sourceId);
        } catch (Exception x) {
            //TODO proper error handling
            x.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        IndexCheckpointConsumer consumer = new IndexCheckpointConsumer("foundry.indexCheckpoint");
        String configFile = "consumers-cfg.xml";
        try {
            consumer.startup(configFile);

            consumer.handleMessages(consumer);

            // for TEST
            // consumer.handle("20140528", "nlx_152590");
            // consumer.handle("20140605", "nlx_999999");

            System.out.print("Press a key to exit:");
            System.in.read();
        } finally {
            consumer.shutdown();
        }

    }

    @Override
    public String getId() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }
}
