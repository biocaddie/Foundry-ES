package org.neuinfo.foundry.consumers.jms.consumers;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.neuinfo.foundry.common.ingestion.DocumentIngestionService;
import org.neuinfo.foundry.common.ingestion.GridFSService;
import org.neuinfo.foundry.consumers.common.Constants;
import org.neuinfo.foundry.consumers.common.IDGenerator;
import org.neuinfo.foundry.consumers.common.ServiceFactory;
import org.neuinfo.foundry.consumers.plugin.IPlugin;
import org.neuinfo.foundry.consumers.plugin.Result;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import java.util.Iterator;
import java.util.List;

/**
 * Created by bozyurt on 12/8/15.
 */
public class CompositeJavaPluginConsumer extends CompositeConsumerSupport implements MessageListener {
    private String id;
    private String name;
    GridFSService gridFSService;
    DocumentIngestionService dis;
    MessagePublisher messagePublisher;
    private final static Logger logger = Logger.getLogger(CompositeJavaPluginConsumer.class);

    public CompositeJavaPluginConsumer(String queueName) {
        super(queueName);
        gridFSService = new GridFSService();
    }

    @Override
    public void startup(String configFile) throws Exception {
        super.startup(configFile);
        gridFSService.start(this.config);
        messagePublisher = new MessagePublisher(this.config.getBrokerURL());
        dis = ServiceFactory.getInstance(configFile).createDocumentIngestionService();
    }

    @Override
    public void shutdown() {
        if (gridFSService != null) {
            gridFSService.shutdown();
        }
        if (messagePublisher != null) {
            messagePublisher.close();
        }
        if (dis != null) {
            dis.shutdown();
        }
        super.shutdown();
    }

    @Override
    public String getId() {
        if (id == null) {
            id = String.valueOf(IDGenerator.getInstance().getNextId());
        }
        return id;
    }


    @Override
    public String getName() {
        return null;
    }

    void handle(String objectId,String collectionName4Record ) throws Exception {
        DB db = mongoClient.getDB(super.mongoDbName);
        String theCollection = collectionName4Record != null ? collectionName4Record : getCollectionName();
        DBCollection collection = db.getCollection(theCollection);
        BasicDBObject query = new BasicDBObject(Constants.MONGODB_ID_FIELD, new ObjectId(objectId));
        DBObject theDoc = collection.findOne(query);
        if (theDoc != null) {
            DBObject pi = (DBObject) theDoc.get("Processing");
            if (pi != null) {
                logger.info("pi:" + pi);
                String status = (String) pi.get("status");
                BasicDBObject origDoc = (BasicDBObject) theDoc.get("OriginalDoc");
                if (origDoc != null && status != null && status.equals(getInStatus())) {
                    List<IPlugin> plugins = getPlugins();
                    for (IPlugin plugin : plugins) {
                        plugin.setGridFSService(this.gridFSService);
                        plugin.setDocumentIngestionService(this.dis);
                    }
                    try {
                        Result result;
                        boolean hasChange = false;
                        boolean hasError = false;
                        for (IPlugin plugin : plugins) {

                            result = plugin.handle(theDoc);
                            if (result.getStatus() == Result.Status.OK_WITH_CHANGE) {
                                hasChange = true;
                            } else if (result.getStatus() == Result.Status.ERROR
                                    || result.getStatus() == Result.Status.NONE) {
                                hasError = true;
                                break;
                            }
                            theDoc = result.getDocWrapper();
                        }
                        if (hasError) {
                            pi.put("status", "error");
                            logger.info("updating status to error");
                            collection.update(query, theDoc);
                            messagePublisher.sendMessage(objectId, "error", theCollection);
                        } else if (hasChange) {
                            pi = (DBObject) theDoc.get("Processing");
                            pi.put("status", getOutStatus());
                            logger.info("updating document");
                            collection.update(query, theDoc);
                            messagePublisher.sendMessage(objectId, getOutStatus(), theCollection);
                        } else {
                            // no changes
                            pi.put("status", getOutStatus());
                            logger.info("updating status to " + getOutStatus());
                            collection.update(query, theDoc);
                            messagePublisher.sendMessage(objectId, getOutStatus(), theCollection);
                        }

                    } catch (Throwable t) {
                        logger.error("handle", t);
                        t.printStackTrace();
                        if (pi != null) {
                            pi.put("status", "error");
                            logger.info("updating status to error");
                            collection.update(query, theDoc);
                            messagePublisher.sendMessage(objectId, "error", theCollection);
                        }

                    }
                }
            }
        }
    }

    @Override
    public void onMessage(Message message) {
        try {
            ObjectMessage om = (ObjectMessage) message;
            String payload = (String) om.getObject();
            if (logger.isInfoEnabled()) {
                logger.info("payload:" + payload);
            }
            JSONObject json = new JSONObject(payload);
            String status = json.getString("status");
            String objectId = json.getString("oid");
            String collectionName4Record = null;
            if (json.has("collectionName")) {
                collectionName4Record = json.getString("collectionName");
            }
            if (logger.isInfoEnabled()) {
                logger.info(String.format("status:%s objectId:%s%n", status, objectId));
            }
            handle(objectId, collectionName4Record);
        } catch (Exception x) {
            logger.error("onMessage", x);
            //TODO proper error handling
            x.printStackTrace();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("CompositeJavaPluginConsumer:[");
        for(Iterator<IPlugin> iter = getPlugins().iterator(); iter.hasNext();) {
            IPlugin plugin = iter.next();
            sb.append(plugin.getPluginName());
            if (iter.hasNext()) {
                sb.append(",");
            }
        }
        sb.append(']');
        return sb.toString();
    }
}
