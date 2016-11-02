package org.neuinfo.foundry.consumers;

import com.mongodb.BasicDBObject;
import junit.framework.TestCase;
import org.neuinfo.foundry.common.ingestion.DocumentIngestionService;
import org.neuinfo.foundry.common.model.Source;
import org.neuinfo.foundry.common.transform.MappingEngine;
import org.neuinfo.foundry.common.transform.TransformationFunctionRegistry;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.jms.consumers.plugins.MappingRegistry;
import org.neuinfo.foundry.consumers.jms.consumers.plugins.TransformationEnhancer;
import org.neuinfo.foundry.consumers.jms.consumers.plugins.TransformationRegistry;
import org.neuinfo.foundry.consumers.plugin.IPlugin;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 11/6/15.
 */
public class TransformationEnhancerTests extends TestCase {
    public TransformationEnhancerTests(String name) {
        super(name);
    }

    public void testTransformationEnhancer() throws Exception {
        Helper helper = new Helper("");
        try {
            helper.startup("consumers-cfg.xml");

            DocumentIngestionService dis = new DocumentIngestionService();
            try {
                dis.start(helper.getConfig());
                List<Source> sourcesWithTransformScript = dis.findSourcesWithTransformScript();
                TransformationRegistry registry = TransformationRegistry.getInstance();
                MappingRegistry mappingRegistry = MappingRegistry.getInstance();

                for (Source source : sourcesWithTransformScript) {
                    registry.register(source.getResourceID(), source.getTransformScript());
                    if (!Utils.isEmpty(source.getMappingScript())) {
                        mappingRegistry.register(source.getResourceID(),
                                new MappingEngine(source.getMappingScript()));
                    }
                }

                //List<BasicDBObject> docWrappers = helper.getDocWrappers("biocaddie-0006");
                // List<BasicDBObject> docWrappers = helper.getDocWrappers("biocaddie-0011", 100);
                //List<BasicDBObject> docWrappers = helper.getDocWrappers("biocaddie-0026", 100);
                //List<BasicDBObject> docWrappers = helper.getDocWrappers("ks-0001", 100);
                List<BasicDBObject> docWrappers = helper.getDocWrappers("biocaddie-0027", 100);
                IPlugin plugin = new TransformationEnhancer();
                plugin.setDocumentIngestionService(dis);
                Map<String, String> options = new HashMap<String, String>(3);
                options.put("addResourceInfo", "false");
                plugin.initialize(options);
                int count = 0;
                for (BasicDBObject docWrapper : docWrappers) {
                    BasicDBObject procDBO = (BasicDBObject) docWrapper.get("Processing");
                    String status = (String) procDBO.get("status");
                    if (status.equals("error")) {
                        System.out.println("handling doc " + count);
                        Result result = plugin.handle(docWrapper);
                        count++;
                        if (count > 0) {
                            break;
                        }
                    }
                }


            } finally {
                dis.shutdown();
            }
        } finally {
            helper.shutdown();
        }
    }
}
