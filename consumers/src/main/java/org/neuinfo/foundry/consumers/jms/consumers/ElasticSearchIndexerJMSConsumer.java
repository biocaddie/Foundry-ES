package org.neuinfo.foundry.consumers.jms.consumers;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.apache.commons.cli.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.ElasticSearchUtils;
import org.neuinfo.foundry.consumers.common.Constants;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import java.net.URI;

/**
 * @author I . Burak Ozyurt
 */
public class ElasticSearchIndexerJMSConsumer extends JMSConsumerSupport implements MessageListener {
    private String serverURL;
    private String indexPath;
    private final static Logger logger = Logger.getLogger(ElasticSearchIndexerJMSConsumer.class);

    public ElasticSearchIndexerJMSConsumer(String serverURL, String indexPath) {
        super("foundry.index");
        this.indexPath = indexPath;
        this.serverURL = serverURL;
    }


    private void indexDoc(String objectId) {
        logger.info("in indexDoc");
        DB db = mongoClient.getDB(super.mongoDbName);

        DBCollection records = db.getCollection("records");

        BasicDBObject query = new BasicDBObject(Constants.MONGODB_ID_FIELD, new ObjectId(objectId));

        DBObject theDoc = records.findOne(query);
        if (theDoc != null) {
            //System.out.println("theDoc:" + theDoc);
            // DBObject pi = (DBObject) theDoc.get("sourceInfo"); // orig
            DBObject pi = (DBObject) theDoc.get("Processing");


            if (pi != null) {
                System.out.println("pi:" + pi);
                String status = (String) pi.get("status");
                if (status.equals("index")) {
                    String docId = (String) pi.get("docId");

                    DBObject doc2Index = (DBObject) theDoc.get("OriginalDoc");
                    String jsonDocStr = doc2Index.toString();
                    final DBObject data = (DBObject) theDoc.get("Data");
                    DBObject keywords = (DBObject) data.get("keywords");
                    if (keywords != null) {
                        final JSONArray keywordJSON = new JSONArray(keywords.toString());
                        JSONObject js = new JSONObject();
                        js.put("keywords", keywordJSON);
                        js.put("content", new JSONObject(jsonDocStr));
                        jsonDocStr = js.toString();
                    }
                    try {
                        boolean ok = send2ElasticSearch(jsonDocStr, docId);
                        if (ok) {
                            pi.put("status", "indexed");
                            records.update(query, theDoc);
                            System.out.println("updated doc status");
                        } else {

                            //FIXME handle exception
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            }
        } else {
            logger.warn("Cannot find object with id:" + objectId);
        }
    }

    public static String ensureIndexPathStartsWithSlash(String indexPath) {
        if (!indexPath.startsWith("/")) {
            indexPath = "/" + indexPath;
        }
        return indexPath;
    }

    boolean deleteIndex(String indexPath) throws Exception {
        //HttpClient client = new DefaultHttpClient();
        HttpClient client = HttpClientBuilder.create().build();
        URIBuilder builder = new URIBuilder(serverURL);
        indexPath = ensureIndexPathStartsWithSlash(indexPath);
        builder.setPath(indexPath);
        URI uri = builder.build();
        System.out.println("uri:" + uri);
        return ElasticSearchUtils.sendDeleteRequest(client, uri);
    }


    private boolean send2ElasticSearch(String jsonDocStr, String docId) throws Exception {
        //HttpClient client = new DefaultHttpClient();
        HttpClient client = HttpClientBuilder.create().build();

        URIBuilder builder = new URIBuilder(serverURL);
        // "http://localhost:9200/");
        indexPath = ensureIndexPathStartsWithSlash(indexPath);
        builder.setPath(indexPath + "/" + docId);  //"/nif/cinergi/" + docId);
        URI uri = builder.build();
        HttpPut httpPut = new HttpPut(uri);
        boolean ok = false;
        try {
            httpPut.addHeader("Accept", "application/json");
            httpPut.addHeader("Content-Type", "application/json");
            StringEntity entity = new StringEntity(jsonDocStr, "UTF-8");
            httpPut.setEntity(entity);
            final HttpResponse response = client.execute(httpPut);
            if (response.getStatusLine().getStatusCode() == 200) {
                ok = true;
            } else {
                System.out.println(response.getStatusLine());
            }

        } finally {
            if (httpPut != null) {
                httpPut.releaseConnection();
            }
        }
        return ok;
    }

    public void onMessage(Message message) {
        try {
            ObjectMessage om = (ObjectMessage) message;
            String payload = (String) om.getObject();
            System.out.println("payload:" + payload);
            JSONObject json = new JSONObject(payload);

            String status = json.getString("status");
            String objectId = json.getString("oid");
            System.out.format("status:%s objectId:%s%n", status, objectId);
            indexDoc(objectId);
        } catch (Exception x) {
            //TODO proper error handling
            x.printStackTrace();
        }
    }

    public static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ElasticSearchIndexerJMSConsumer", options);
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {
        Option help = new Option("h", "print this message");
        Option ipOption = Option.builder("p").argName("index-path").hasArg()
                .desc("Elasticsearch index-path for REST api").build();
        Option urlOption = Option.builder("u").argName("server-url").hasArg()
                .desc("Elasticsearch server url such as 'http://localhost:9200'").build();
        Option cmdOption = Option.builder("c").argName("command").hasArg()
                .desc("command (one of [i,d] where d is for delete index)").build();
        urlOption.setRequired(true);
        ipOption.setRequired(true);
        cmdOption.setRequired(true);
        Options options = new Options();
        options.addOption(help);
        options.addOption(cmdOption);
        options.addOption(ipOption);
        options.addOption(urlOption);
        CommandLineParser cli = new DefaultParser();
        CommandLine line = null;
        try {
            line = cli.parse(options, args);
        } catch (Exception x) {
            System.err.println(x.getMessage());
            usage(options);
        }

        if (line.hasOption("h")) {
            usage(options);
        }
        if (!line.hasOption("p") || !line.hasOption("c") || !line.hasOption("u")) {
            usage(options);
        }
        String indexPath = line.getOptionValue("p");
        String serverUrl = line.getOptionValue("u");
        String cmd = line.getOptionValue("c");

        ElasticSearchIndexerJMSConsumer consumer = new ElasticSearchIndexerJMSConsumer(serverUrl, indexPath);
        if (cmd.equals("d")) {
            final boolean ok = consumer.deleteIndex(indexPath);
            if (ok) {
                System.out.println("Elasticsearch index '" + indexPath + "' is deleted.");
            }
        } else {
            try {
                String configFile = "consumers-cfg.xml";
                consumer.startup(configFile);
                consumer.handleMessages(consumer);

                System.out.print("Press a key to exit:");
                System.in.read();
            } finally {
                consumer.shutdown();
            }
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
