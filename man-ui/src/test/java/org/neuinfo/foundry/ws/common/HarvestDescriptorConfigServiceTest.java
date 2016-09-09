package org.neuinfo.foundry.ws.common;

import junit.framework.TestCase;
import org.json.JSONObject;
import org.neuinfo.foundry.common.ingestion.Configuration;
import org.neuinfo.foundry.common.ingestion.HarvestDescriptorConfigService;
import org.neuinfo.foundry.common.model.IngestConfig;
import org.neuinfo.foundry.common.util.Utils;

import java.io.File;
import java.util.List;

/**
 * Created by bozyurt on 8/25/15.
 */
public class HarvestDescriptorConfigServiceTest extends TestCase {
    public HarvestDescriptorConfigServiceTest(String name) {
        super(name);
    }

    public void testPopulate() throws Exception {
        Configuration conf = ConfigLoader2.load("man-ui-cfg.xml", false);
        HarvestDescriptorConfigService service = new HarvestDescriptorConfigService();
        try {
            service.start(conf);
            String homeDir = System.getProperty("user.home");
            //File dir = new File(homeDir + "/work/Foundry-ES/consumers/etc/ingestor-configs");
            File dir = new File(homeDir + "/dev/java/Foundry-ES/consumers/etc/ingestor-configs");
            List<File> files = Utils.findAllFilesMatching(dir, new Utils.RegexFileNameFilter("\\.json$"));
            for(File jsonFile : files) {
                String jsonStr = Utils.loadAsString(jsonFile.getAbsolutePath());
                JSONObject json = new JSONObject(jsonStr);
                IngestConfig ic = IngestConfig.fromJSON(json);
                service.saveIngestConfig(ic);
            }

        } finally {
            service.shutdown();
        }

    }
}
