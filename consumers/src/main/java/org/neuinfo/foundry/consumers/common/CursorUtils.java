package org.neuinfo.foundry.consumers.common;

import org.json.JSONObject;
import org.neuinfo.foundry.common.util.JSONPathProcessor;
import org.neuinfo.foundry.common.util.JSONPathProcessor2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bozyurt on 2/28/17.
 */
public class CursorUtils {

    public static List<JSONObject> filter(String refValue, String resetJsonPath, List<JSONObject> records) throws Exception {
        JSONPathProcessor2 processor = new JSONPathProcessor2();
        JSONPathProcessor.Path path = processor.compile(resetJsonPath);
        List<JSONObject> list = new ArrayList<JSONObject>();
        for(JSONObject json : records) {
            List<JSONPathProcessor2.JPNode> jpNodes = processor.find(path, json);
            if (jpNodes != null && jpNodes.size() == 1) {
                String value = jpNodes.get(0).getValue();
                if (value.equals(refValue)) {
                    list.add(json);
                }
            }
        }
        return list;
    }

    public static String extractStringValue(JSONObject record, JSONPathProcessor.Path path) {
        if (record != null) {
            JSONPathProcessor2 processor = new JSONPathProcessor2();
            List<JSONPathProcessor2.JPNode> jpNodes = processor.find(path, record);
            if (jpNodes != null) {
                JSONPathProcessor2.JPNode jpNode = jpNodes.get(0);
                return jpNode.getValue();
            }
        }
        return null;
    }
}
