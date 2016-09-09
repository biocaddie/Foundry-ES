package org.neuinfo.foundry.ws.common;

import org.json.JSONObject;
import org.neuinfo.foundry.consumers.jms.consumers.plugins.TransformationRegistry;

import java.util.List;

/**
 * Created by bozyurt on 8/31/15.
 */
public class BootstrapHelper {
    MongoService mongoService;

    public BootstrapHelper(MongoService mongoService) {
        this.mongoService = mongoService;
    }

    public void bootstrap() {
        /* not used
        TransformationRegistry registry = TransformationRegistry.getInstance();
        List<JSONObject> sourceSummaries = mongoService.getSourceSummaries();
        for(JSONObject ssJSON : sourceSummaries) {
            String srcId = ssJSON.getString("sourceID");
            if (ssJSON.has("transformScript")) {
                registry.register(srcId, ssJSON.getString("transformScript"));
            }
        }
        */
    }
}
