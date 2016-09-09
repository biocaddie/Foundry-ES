package org.neuinfo.foundry.common.transform;

import org.json.JSONObject;
import org.neuinfo.foundry.common.util.JSONPathProcessor;
import org.neuinfo.foundry.common.util.JSONPathProcessor2;
import org.neuinfo.foundry.common.util.JSONPathProcessor2.JPNode;
import org.neuinfo.foundry.common.util.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 5/4/15.
 */
public class JsonPathFieldExtractor {


    public void extractValue(JSONObject docJson, String jsonPathStr, Map<String, List<String>> nameValueMap) {
        JSONPathProcessor processor = new JSONPathProcessor();
        try {
            List<Object> list = processor.find(jsonPathStr, docJson);
            if (list != null && !list.isEmpty()) {
                List<String> values = new ArrayList<String>(list.size());
                for (Object o : list) {
                    System.out.println(o);
                    values.add(o.toString());
                }
                nameValueMap.put(jsonPathStr, values);
            }
        } catch (Exception e) {
            System.out.println("jsonPathStr:" + jsonPathStr);
            e.printStackTrace();
        }
    }

    public void extractValue2(JSONObject docJson, String jsonPathStr, Map<String, List<JPNode>> nameValueMap) {
        JSONPathProcessor2 processor = new JSONPathProcessor2();
        try {
            List<JPNode> list = processor.find(jsonPathStr, docJson);
            if (list != null && !list.isEmpty()) {
                /*
                for (JPNode jpNode : list) {
                    System.out.println(jpNode);
                }
                */
                nameValueMap.put(jsonPathStr, list);
            }
        } catch (Exception e) {
            System.out.println("jsonPathStr:" + jsonPathStr);
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        String jsonStr = Utils.loadAsString("/tmp/pdb_xml_record.json");
        JSONObject docJSON = new JSONObject(jsonStr);
        JsonPathFieldExtractor extractor = new JsonPathFieldExtractor();

        Map<String, List<String>> nameValueMap = new HashMap<String, List<String>>(7);

        //  extractor.extractValue(docJSON, "$..'PDBx:citation'[?(@.'@id' = 'primary')].'PDBx:title'.'_$'", nameValueMap);
        extractor.extractValue(docJSON, "$..'PDBx:citation_author'[?(@.'@citation_id'='primary')].'@name'", nameValueMap);

    }
}
