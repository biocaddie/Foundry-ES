package org.neuinfo.foundry.common.util;

import org.json.JSONObject;
import org.neuinfo.foundry.common.config.Configuration;
import org.neuinfo.foundry.common.config.WorkflowMapping;
import org.neuinfo.foundry.common.model.Source;

import java.util.Date;
import java.util.List;

/**
 * Created by bozyurt on 1/13/16.
 */
public class MessagingUtils {



    public static JSONObject prepareMessageBody(String cmd, Source source, List<WorkflowMapping> workflowMappings) throws Exception {
        // check if source has a valid workflow
        WorkflowMapping wm = hasValidWorkflow(source, workflowMappings);
        if (wm == null) {
            throw new Exception("No matching workflow for source " + source.getResourceID());
        }
        String batchId = Utils.prepBatchId(new Date());
        JSONObject json = new JSONObject();
        json.put("cmd", cmd);
        json.put("batchId", batchId);
        json.put("srcNifId", source.getResourceID());
        json.put("dataSource", source.getDataSource());
        json.put("ingestConfiguration", source.getIngestConfiguration());
        json.put("contentSpecification", source.getContentSpecification());

        json.put("transformScript", source.getTransformScript());
        json.put("mappingScript", source.getMappingScript());

        json.put("ingestorOutStatus", wm.getIngestorOutStatus());
        json.put("updateOutStatus", wm.getUpdateOutStatus());

        return json;
    }

    public static WorkflowMapping hasValidWorkflow(Source source, List<WorkflowMapping> workflowMappings) {
        List<String> workflowSteps = source.getWorkflowSteps();
        for (WorkflowMapping wm : workflowMappings) {
            if (workflowSteps.size() == wm.getSteps().size()) {
                for (int i = 0; i < workflowSteps.size(); i++) {
                    String step = workflowSteps.get(i);
                    if (!step.equals(wm.getSteps().get(i))) {
                        break;
                    }
                }
                return wm;
            }
        }
        return null;
    }

}
