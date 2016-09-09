package org.neuinfo.foundry.common.config;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.jdom2.Element;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bozyurt on 10/30/14.
 */
public class WorkflowMapping {
    String workflowName;
    List<String> steps;
    String ingestorOutStatus;
    /**
     * if the document is updated, some steps such as doc id generation can be skipped
     */
    String updateOutStatus;

    public WorkflowMapping(String workflowName) {
        this.workflowName = workflowName;
    }

    public String getWorkflowName() {
        return workflowName;
    }

    public List<String> getSteps() {
        return steps;
    }

    public String getIngestorOutStatus() {
        return ingestorOutStatus;
    }

    public String getUpdateOutStatus() {
        return updateOutStatus;
    }

    public static WorkflowMapping fromXml(Element elem) throws Exception {
        String name = elem.getAttributeValue("name");
        WorkflowMapping wm = new WorkflowMapping(name);
        List<Element> children = elem.getChildren("step");
        wm.steps = new ArrayList<String>(children.size());
        for (Element el : children) {
            wm.steps.add(el.getTextTrim());

        }
        wm.ingestorOutStatus = elem.getAttributeValue("ingestorOutStatus");
        wm.updateOutStatus = elem.getAttributeValue("updateOutStatus");
        return wm;
    }


    public JSONObject toJSON() {
        JSONObject json = new JSONObject();
        json.put("workflowName", workflowName);
        json.put("ingestorOutStatus", ingestorOutStatus);
        json.put("updateOutStatus", updateOutStatus);
        JSONArray jsArr = new JSONArray();
        json.put("steps", jsArr);
        for (String step : steps) {
            jsArr.put(step);
        }
        return json;
    }

    public static WorkflowMapping fromDBObject(DBObject wfmDBO) {
        String workflowName = (String) wfmDBO.get("workflowName");
        String ingestorOutStatus = (String) wfmDBO.get("ingestorOutStatus");
        String updateOutStatus = (String) wfmDBO.get("updateOutStatus");
        BasicDBList steps = (BasicDBList) wfmDBO.get("steps");
        WorkflowMapping wm = new WorkflowMapping(workflowName);
        wm.ingestorOutStatus = ingestorOutStatus;
        wm.updateOutStatus = updateOutStatus;
        wm.steps = new ArrayList<String>(steps.size());
        for (int i = 0; i < steps.size(); i++) {
            wm.steps.add((String) steps.get(i));
        }
        return wm;
    }


}
