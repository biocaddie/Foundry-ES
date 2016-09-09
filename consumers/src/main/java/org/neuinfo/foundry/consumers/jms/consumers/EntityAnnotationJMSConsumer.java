package org.neuinfo.foundry.consumers.jms.consumers;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.apache.commons.cli.*;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.JSONPathProcessor;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.common.Constants;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import java.net.URI;
import java.util.*;

/**
 * Created by bozyurt on 6/4/14.
 */
public class EntityAnnotationJMSConsumer extends JMSConsumerSupport implements MessageListener {
    private final static Logger logger = Logger.getLogger(EntityAnnotationJMSConsumer.class);
    private final static String serviceURL = "http://tikki.neuinfo.org:9000/scigraph/annotations/entities";
    private List<String> jsonPaths;

    public EntityAnnotationJMSConsumer(String queueName, Properties props) {
        super(queueName);

        String pathStr = props.getProperty("field.jsonpath.list");
        String[] toks = pathStr.split("\\s*,\\s*");
        jsonPaths = Arrays.asList(toks);
    }

    private void addKeywords(String objectId) {
        logger.info("in addKeywords");
        DB db = mongoClient.getDB(super.mongoDbName);

        DBCollection records = db.getCollection("records");

        BasicDBObject query = new BasicDBObject(Constants.MONGODB_ID_FIELD, new ObjectId(objectId));

        DBObject theDoc = records.findOne(query);
        if (theDoc != null) {
            //System.out.println("theDoc:" + theDoc);
            DBObject pi = (DBObject) theDoc.get("Processing");
            String status = null;
            if (pi != null) {
                status = (String) pi.get("status");
            }

            BasicDBObject origDoc = (BasicDBObject) theDoc.get("OriginalDoc");
            if (origDoc != null && status != null && status.equals("new")) {
                final JSONObject json = JSONUtils.toJSON(origDoc, false);
                try {
                    JSONPathProcessor processor = new JSONPathProcessor();
                    Map<String, Keyword> keywordMap = new LinkedHashMap<String, Keyword>();
                    for (String jsonPath : jsonPaths) {
                        final List<Object> objects = processor.find(jsonPath, json);
                        if (objects != null && !objects.isEmpty()) {
                            String text2Annotate = (String) objects.get(0);
                            annotateEntities(jsonPath, text2Annotate, keywordMap);
                        }
                    }
                    JSONArray jsArr = new JSONArray();
                    for (EntityAnnotationJMSConsumer.Keyword keyword : keywordMap.values()) {
                        jsArr.put(keyword.toJSON());
                    }
                    DBObject data = (DBObject) theDoc.get("Data");
                    data.put("keywords", JSONUtils.encode(jsArr));
                    // pi.put("status", "index_cp");
                    pi.put("status", "transform");
                    final UUID uuid = UUID.randomUUID();
                    pi.put("docId", uuid.toString());

                    records.update(query, theDoc);
                    System.out.println("updated");
                } catch (Exception x) {
                    x.printStackTrace();
                    if (pi != null) {
                        pi.put("status", "error");
                        System.out.println("updating");
                        records.update(query, theDoc);
                    }
                }
            }
        } else {
            logger.warn("Cannot find object with id:" + objectId);
        }
    }

    public static void annotateEntities(String contentLocation, String text, Map<String, Keyword> keywordMap) throws Exception {
        HttpClient client = new DefaultHttpClient();
        URIBuilder builder = new URIBuilder(serviceURL);
        builder.setParameter("content", text);
        // minLength=4&longestOnly=true&includeAbbrev=false&includeAcronym=false&includeNumbers=false&callback=fn
        builder.setParameter("minLength", "4");
        builder.setParameter("longestOnly", "true");
        builder.setParameter("includeAbbrev", "false");
        builder.setParameter("includeNumbers", "false");

        URI uri = builder.build();
        HttpGet httpGet = new HttpGet(uri);
        // httpGet.addHeader("Content-Type", "application/json");
        httpGet.addHeader("Accept", "application/json");
        try {
            final HttpResponse response = client.execute(httpGet);
            final HttpEntity entity = response.getEntity();
            if (entity != null) {
                String jsonStr = EntityUtils.toString(entity);
                System.out.println(jsonStr);
                JSONArray jsArr = new JSONArray(jsonStr);
                for (int i = 0; i < jsArr.length(); i++) {
                    final JSONObject json = jsArr.getJSONObject(i);
                    if (json.has("token")) {
                        JSONObject tokenObj = json.getJSONObject("token");
                        String id = tokenObj.getString("id");
                        final JSONArray terms = tokenObj.getJSONArray("terms");
                        if (terms.length() > 0) {
                            int start = json.getInt("start");
                            int end = json.getInt("end");
                            String term = terms.getString(0);
                            Keyword keyword = keywordMap.get(term);
                            if (keyword == null) {
                                keyword = new Keyword(term);
                                keywordMap.put(term, keyword);
                            }
                            EntityInfo ei = new EntityInfo(contentLocation, id, start, end);
                            keyword.addEntityInfo(ei);
                        }
                    }
                }
            }
        } finally {
            if (httpGet != null) {
                httpGet.releaseConnection();
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

    public static class Keyword {
        private String term;
        List<EntityInfo> entityInfos = new LinkedList<EntityInfo>();

        public Keyword(String term) {
            this.term = term;
        }

        public void addEntityInfo(EntityInfo ei) {
            entityInfos.add(ei);
        }

        public String getTerm() {
            return term;
        }

        public List<EntityInfo> getEntityInfos() {
            return entityInfos;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Keyword{");
            sb.append("term='").append(term).append('\'');
            sb.append(", entityInfos=").append(entityInfos);
            sb.append('}');
            return sb.toString();
        }

        public JSONObject toJSON() {
            JSONObject js = new JSONObject();
            js.put("term", term);
            JSONArray jsArr = new JSONArray();
            js.put("entityInfos", jsArr);
            for (EntityInfo ei : entityInfos) {
                jsArr.put(ei.toJSON());
            }
            return js;
        }
    }

    public static class EntityInfo {
        final String contentLocation;
        final String id;
        final int start;
        final int end;

        public EntityInfo(String contentLocation, String id, int start, int end) {
            this.contentLocation = contentLocation;
            this.id = id;
            this.start = start;
            this.end = end;
        }

        public String getContentLocation() {
            return contentLocation;
        }

        public String getId() {
            return id;
        }

        public int getStart() {
            return start;
        }

        public int getEnd() {
            return end;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("EntityInfo{");
            sb.append("contentLocation='").append(contentLocation).append('\'');
            sb.append(", id='").append(id).append('\'');
            sb.append(", start=").append(start);
            sb.append(", end=").append(end);
            sb.append('}');
            return sb.toString();
        }

        public JSONObject toJSON() {
            JSONObject js = new JSONObject();
            js.put("contentLocation", contentLocation);
            js.put("id", id);
            js.put("start", start);
            js.put("end", end);
            return js;
        }
    }


    @Override
    public void onMessage(Message message) {
        try {
            ObjectMessage om = (ObjectMessage) message;
            String payload = (String) om.getObject();
            System.out.println("payload:" + payload);
            JSONObject json = new JSONObject(payload);
            String status = json.getString("status");
            String objectId = json.getString("oid");
            System.out.format("status:%s objectId:%s%n", status, objectId);
            addKeywords(objectId);
        } catch (Exception x) {
            //TODO proper error handling
            x.printStackTrace();
        }
    }

    public static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("EntityAnnotationJMSConsumer", options);
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {
        Option help = new Option("h", "print this message");
        Option propsOption = Option.builder("p").argName("entity-props-file").hasArg().
                desc("config properties file for the entity annotation consumer").build();
        propsOption.setRequired(true);
        Options options = new Options();
        options.addOption(help);
        options.addOption(propsOption);
        CommandLineParser cli = new DefaultParser();
        CommandLine line = null;
        try {
            line = cli.parse(options, args);
        } catch (Exception x) {
            System.err.println(x.getMessage());
            usage(options);
        }
        if (line == null || line.hasOption("h")) {
            usage(options);
        }

        String propsPath = line.getOptionValue('p');
        Properties props = Utils.loadPropertiesFromPath(propsPath);

        EntityAnnotationJMSConsumer consumer = new EntityAnnotationJMSConsumer("foundry.new", props);
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
