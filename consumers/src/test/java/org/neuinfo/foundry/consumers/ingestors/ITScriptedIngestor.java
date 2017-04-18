package org.neuinfo.foundry.consumers.ingestors;

import org.junit.Test;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.common.Parameters;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.ScriptedIngestor;
import org.neuinfo.foundry.consumers.tool.IngestionUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by bozyurt on 3/10/17.
 */
public class ITScriptedIngestor {

    @Test
    public void testJoin() throws Exception {
        String script = loadScriptFromYaml("testdata/neurosynth_ingestion_script.yml");
        Map<String, String> options = new HashMap<String, String>(7);
        options.put("script", script);
        ScriptedIngestor ingestor = new ScriptedIngestor();
        ingestor.initialize(options);
        IngestionUtils.ingest(ingestor, "/tmp/neurosynth_record.json", 5);
    }

    @Test
    public void testOnDemandJoin() throws Exception {
        String script = loadScriptFromYaml("testdata/xnat_ingestion_script.yml");
        String pwd = Parameters.getInstance().getParam("xnat.pwd");
        script = script.replaceAll("\\<PWD\\>",pwd);
        Map<String, String> options = new HashMap<String, String>(7);
        options.put("script", script);
        ScriptedIngestor ingestor = new ScriptedIngestor();
        ingestor.initialize(options);
        IngestionUtils.ingest(ingestor, "/tmp/xnat_record.json", 5);
    }

    public static String loadScriptFromYaml(String yamlPath) {
        Yaml yaml = new Yaml();
        InputStream in = null;
        try {
            in = ITScriptedIngestor.class.getClassLoader().getResourceAsStream(yamlPath);
            Map<String, Object> map = (Map<String, Object>) yaml.load(in);
            return (String) map.get("script");
        } finally {
            Utils.close(in);
        }

    }
}
