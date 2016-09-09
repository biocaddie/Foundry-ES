package org.neuinfo.foundry.consumers.jms.consumers.plugins;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.neuinfo.foundry.common.ingestion.DocumentIngestionService;
import org.neuinfo.foundry.common.ingestion.GridFSService;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.neuinfo.foundry.consumers.common.ConsumerUtils;
import org.neuinfo.foundry.consumers.plugin.IPlugin;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.util.Map;

/**
 * Created by bozyurt on 5/6/15.
 */
public class ElasticSearchIndexer implements IPlugin {
    private String serverURL;
    private String indexPath;
    private final static Logger log = Logger.getLogger(ElasticSearchIndexer.class);

    @Override
    public void setDocumentIngestionService(DocumentIngestionService dis) {
        // no op
    }

    @Override
    public void setGridFSService(GridFSService gridFSService) {
        // no op
    }

    @Override
    public void initialize(Map<String, String> options) throws Exception {
        this.serverURL = options.get("serverURL");
        this.indexPath = options.get("indexPath");
        Assertion.assertNotNull(this.serverURL);
        Assertion.assertNotNull(this.indexPath);
    }

    @Override
    public Result handle(DBObject docWrapper) {
        try {
            BasicDBObject procDBO = (BasicDBObject) docWrapper.get("Processing");
            String docId = procDBO.getString("docId");
            BasicDBObject data = (BasicDBObject) docWrapper.get("Data");
            BasicDBObject transformedRecDBO = (BasicDBObject) data.get("transformedRec");
            String jsonDocStr;
            JSONObject js = JSONUtils.toJSON(transformedRecDBO, true);

            jsonDocStr = js.toString();
            boolean ok = ConsumerUtils.send2ElasticSearch(jsonDocStr, docId, indexPath, serverURL);
            if (ok) {
                return new Result(docWrapper, Result.Status.OK_WITHOUT_CHANGE);
            } else {
                Result r = new Result(docWrapper, Result.Status.ERROR);
                r.setErrMessage("Error indexing document with docId:" + docId);
                return r;
            }
        } catch (Throwable t) {
            log.error("handle",t);
            t.printStackTrace();
            Result r = new Result(docWrapper, Result.Status.ERROR);
            r.setErrMessage(t.getMessage());
            return r;
        }
    }

    @Override
    public String getPluginName() {
        return "ElasticSearchIndexer";
    }
}
