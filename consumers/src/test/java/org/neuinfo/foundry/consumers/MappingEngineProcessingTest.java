package org.neuinfo.foundry.consumers;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import junit.framework.TestCase;
import org.json.JSONObject;
import org.neuinfo.foundry.common.ingestion.DocumentIngestionService;
import org.neuinfo.foundry.common.transform.JsonPathFieldExtractor;
import org.neuinfo.foundry.common.transform.MappingEngine;
import org.neuinfo.foundry.common.transform.TransformMappingUtils;
import org.neuinfo.foundry.common.transform.TransformationEngine;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.neuinfo.foundry.consumers.common.ServiceFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 9/14/15.
 */
public class MappingEngineProcessingTest extends TestCase {
    public MappingEngineProcessingTest(String name) {
        super(name);
    }

    public void testMapping4Errors() throws Exception {
        String rootDir = "/var/burak/dev/java/Foundry-Data";
        String transformScript = TransformMappingUtils.loadTransformMappingScript(rootDir + "/transformations/lincs_ds_result.trs");
        String mappingScript = TransformMappingUtils.loadTransformMappingScript(rootDir + "/mappings/lincs_ds_result.ms");
        Helper helper = new Helper("");
        TransformationEngine trEngine = new TransformationEngine(transformScript);

        MappingEngine mappingEngine = new MappingEngine(mappingScript);
        DocumentIngestionService dis = null;
        try {
            String id = "55f7d2ade4b0b636599fc5b8";

            String configFile = "consumers-cfg.xml";
            helper.startup(configFile);
            BasicDBObject docWrapper = helper.getDocWrapper(id, "nifRecords");
            assertNotNull(docWrapper);
            ServiceFactory serviceFactory = ServiceFactory.getInstance(configFile);
            dis = serviceFactory.createDocumentIngestionService();
            mappingEngine.bootstrap(dis);
            DBObject originalDoc = (DBObject) docWrapper.get("OriginalDoc");
            JSONObject json = JSONUtils.toJSON((BasicDBObject) originalDoc, false);
            JSONObject transformedJson = new JSONObject();
            trEngine.transform(json, transformedJson);
            mappingEngine.map(transformedJson);

            System.out.println(transformedJson.toString(2));

        } finally {
            if (dis != null) {
                dis.shutdown();
            }
            helper.shutdown();
        }
    }

    public void testMapping() throws Exception {
        String rootDir = "/var/burak/dev/java/Foundry-Data";
        String transformScript = TransformMappingUtils.loadTransformMappingScript(rootDir + "/transformations/lincs_ds_result.trs");
        String mappingScript = TransformMappingUtils.loadTransformMappingScript(rootDir + "/mappings/lincs_ds_result.ms");

        String origDataJsonFile = rootDir + "/SampleData/LINCS/datasets/lincs_ds_result_1.json";
        TransformationEngine trEngine = new TransformationEngine(transformScript);

        MappingEngine mappingEngine = new MappingEngine(mappingScript);

        JSONObject json = JSONUtils.loadFromFile(origDataJsonFile);
        DocumentIngestionService dis = null;
        try {
            ServiceFactory serviceFactory = ServiceFactory.getInstance("consumers-cfg.xml");
            dis = serviceFactory.createDocumentIngestionService();
            mappingEngine.bootstrap(dis);
            JSONObject transformedJson = new JSONObject();
            trEngine.transform(json, transformedJson);
            mappingEngine.map(transformedJson);

            System.out.println(transformedJson.toString(2));

        } finally {
            if (dis != null) {
                dis.shutdown();
            }
        }
    }
}
