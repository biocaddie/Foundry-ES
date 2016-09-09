package org.neuinfo.foundry.common.ingestion;

import com.mongodb.*;
import org.json.JSONObject;
import org.neuinfo.foundry.common.config.Workflow;
import org.neuinfo.foundry.common.config.WorkflowMapping;
import org.neuinfo.foundry.common.util.JSONUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 1/26/16.
 */
public class ConfigService extends BaseIngestionService {
    public void setMongoClient(MongoClient mc) {
        super.mongoClient = mc;
    }

    public void setDbName(String dbName) {
        super.dbName = dbName;
    }

    public List<WorkflowMapping> getWorkflowMappings() {
        DB db = mongoClient.getDB(dbName);
        DBCollection collection = db.getCollection("workflowMappings");
        DBCursor cursor = collection.find();
        List<WorkflowMapping> wmList = new ArrayList<WorkflowMapping>(10);
        try {
            while (cursor.hasNext()) {
                DBObject dbo = cursor.next();
                WorkflowMapping wfm = WorkflowMapping.fromDBObject(dbo);
                wmList.add(wfm);
            }
        } finally {
            cursor.close();
        }

        return wmList;
    }

    public void saveUpdateWorkflowMappings(List<WorkflowMapping> wmList) {
        DB db = mongoClient.getDB(dbName);
        DBCollection collection = db.getCollection("workflowMappings");
        DBCursor cursor = collection.find();
        Map<String, WorkflowMapping> existingWFMappingMap = new HashMap<String, WorkflowMapping>();
        try {
            while (cursor.hasNext()) {
                DBObject dbo = cursor.next();
                WorkflowMapping wfm = WorkflowMapping.fromDBObject(dbo);
                existingWFMappingMap.put(wfm.getWorkflowName(), wfm);
            }
        } finally {
            cursor.close();
        }
        if (existingWFMappingMap.isEmpty()) {
            for (WorkflowMapping wm : wmList) {
                JSONObject json = wm.toJSON();
                DBObject dbo = JSONUtils.encode(json, true);
                collection.insert(dbo, WriteConcern.SAFE);
            }
        } else {
            for (WorkflowMapping wm : wmList) {
                WorkflowMapping existingWfm = existingWFMappingMap.get(wm.getWorkflowName());
                if (existingWfm == null) {
                    JSONObject json = wm.toJSON();
                    DBObject dbo = JSONUtils.encode(json, true);
                    collection.insert(dbo, WriteConcern.SAFE);
                } else {
                    boolean hasChange = false;
                    hasChange |= !wm.getIngestorOutStatus().equals(existingWfm.getIngestorOutStatus());
                    hasChange |= !wm.getUpdateOutStatus().equals(existingWfm.getUpdateOutStatus());
                    if (!hasChange) {
                        if (wm.getSteps().size() != existingWfm.getSteps().size()) {
                            hasChange = true;
                        } else {
                            for (int i = 0; i < wm.getSteps().size(); i++) {
                                if (!wm.getSteps().get(i).equals(existingWfm.getSteps().get(i))) {
                                    hasChange = true;
                                    break;
                                }
                            }
                        }
                    }
                    if (hasChange) {
                        BasicDBObject query = new BasicDBObject("workflowName", existingWfm.getWorkflowName());
                        JSONObject json = wm.toJSON();
                        DBObject dbo = JSONUtils.encode(json, true);
                        collection.update(query, dbo, false, false, WriteConcern.SAFE);
                    }
                }
            }
        }
    }

}
