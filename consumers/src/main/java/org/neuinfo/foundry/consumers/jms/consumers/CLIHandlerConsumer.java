package org.neuinfo.foundry.consumers.jms.consumers;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.jdom2.Element;
import org.json.JSONObject;
import org.neuinfo.foundry.common.config.ConsumerConfig;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.common.util.XML2JSONConverter;
import org.neuinfo.foundry.consumers.common.ConfigLoader;
import org.neuinfo.foundry.consumers.common.Configuration;
import org.neuinfo.foundry.consumers.common.Constants;

import javax.jms.Message;
import javax.jms.MessageListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bozyurt on 10/3/14.
 */
public class CLIHandlerConsumer extends JMSConsumerSupport implements MessageListener {
    List<ConsumerConfig.HandlerConfig> handlerConfigList;

    public CLIHandlerConsumer(String queueName, List<ConsumerConfig.HandlerConfig> handlerConfigList) {
        super(queueName);
        this.handlerConfigList = handlerConfigList;
    }

    void handle(String objectId) throws Exception {
        DB db = mongoClient.getDB(super.mongoDbName);

        DBCollection records = db.getCollection(getCollectionName());

        BasicDBObject query = new BasicDBObject(Constants.MONGODB_ID_FIELD, new ObjectId(objectId));

        DBObject theDoc = records.findOne(query);
        if (theDoc != null) {
            DBObject pi = (DBObject) theDoc.get("Processing");
            if (pi != null) {
                System.out.println("pi:" + pi);
                String status = (String) pi.get("status");
                if (status.equals("new")) {
                    DBObject originalDoc = (DBObject) theDoc.get("OriginalDoc");
                    JSONObject json = JSONUtils.toJSON((BasicDBObject) originalDoc, false);
                    JSONObject inJson = json;
                    for(ConsumerConfig.HandlerConfig hc : this.handlerConfigList) {
                        JSONObject outJson = processHandler(hc, inJson, objectId);
                        if (outJson == null) {
                            throw new Exception("Error in handler " + hc.getName());
                        }
                        inJson = outJson;
                    }
                }
            }
        }
    }

    JSONObject processHandler(ConsumerConfig.HandlerConfig hc, JSONObject originalDoc, String objectId) throws Exception {
        List<String> cmdList = new ArrayList<String>(10);
        cmdList.add(hc.getHandler());
        boolean ok = false;
        File inputFile;
        File outFile = null;
        String arg2Format = null;
        if (hc.hasParams()) {
            int numArgs = Utils.getIntValue(hc.getParam("no-args"), -1);
            if (numArgs >= 2) {
                // assumption: first two arguments are input and and output files
                String arg1Type = hc.getParam("arg1-type");
                String arg1Format = hc.getParam("arg1-format");
                Assertion.assertTrue(arg1Type.equals("file"));
                Assertion.assertTrue(arg1Format.equals("xml") || arg1Format.equals("json"));
                String arg2Type = hc.getParam("arg2-type");
                arg2Format = hc.getParam("arg2-format");
                String arg1Name = hc.getParam("arg1-name");
                String arg2Name = hc.getParam("arg2-name");
                Assertion.assertTrue(arg2Type.equals("file"));
                Assertion.assertTrue(arg2Format.equals("xml") || arg2Format.equals("json"));

                inputFile = prepareInputFile(arg1Format, originalDoc, objectId);
                outFile = prepareOutFile(arg2Format, objectId);

                if (arg1Name != null) {
                    cmdList.add(arg1Name);
                }
                cmdList.add(inputFile.getAbsolutePath());
                if (arg2Name != null) {
                    cmdList.add(arg2Name);
                }
                cmdList.add(outFile.getAbsolutePath());
                ok = true;
            }
        }

        if (!ok) {
            return null;
        }
        ProcessBuilder pb = new ProcessBuilder(cmdList);

        Process process = pb.start();
        BufferedReader bin = new BufferedReader(new InputStreamReader(
                process.getErrorStream()));
        String line = null;
        while ((line = bin.readLine()) != null) {
            System.out.println(line);
        }

        int rc = process.waitFor();
        System.out.println("rc:" + rc);
        if (rc == 0) {
            if (arg2Format.equals("json") && outFile != null) {
                JSONObject jsonObject = JSONUtils.loadFromFile(outFile.getAbsolutePath());
                return jsonObject;
            }
        }


        return null;
    }

    File prepareInputFile(String format, JSONObject originalDoc, String objectId) throws Exception {
        File dir = new File("/tmp/consumers");
        if (!dir.isDirectory()) {
            dir.mkdirs();
        }

        File inputFile = null;
        if (format.equals("xml")) {
            inputFile = new File(dir, objectId + ".xml");
            XML2JSONConverter converter = new XML2JSONConverter();
            System.out.println(originalDoc.toString(2));
            Element docEl = converter.toXML(originalDoc);
            Utils.saveXML(docEl, inputFile.getAbsolutePath());
        } else {
            inputFile = new File(dir, objectId + ".json");
        }

        return inputFile;
    }

    File prepareOutFile(String format, String objectId) {
        File dir = new File("/tmp/consumers");
        if (!dir.isDirectory()) {
            dir.mkdirs();
        }
        File outputFile = null;
        if (format.equals("xml")) {
            outputFile = new File(dir, objectId + ".xml");
        } else {
            outputFile = new File(dir, objectId + ".json");
        }
        return outputFile;
    }

    @Override
    public void onMessage(Message message) {
        //TODO
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = ConfigLoader.load("consumers-cfg.xml");
        ConsumerConfig theCF = null;

        for(ConsumerConfig cf : conf.getConsumerConfigs()) {
             if (cf.getName().equals("harvest-iso")) {
                 theCF = cf;
                 break;
             }
        }
        CLIHandlerConsumer consumer = new CLIHandlerConsumer(theCF.getListeningQueueName(), theCF.getHandlerConfigs());
        String configFile = "consumers-cfg.xml";
        try {
            consumer.startup(configFile);

            consumer.handle("542dcde4e4b059f1b4904c85");

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
