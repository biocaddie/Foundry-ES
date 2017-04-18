package org.neuinfo.foundry.consumers.common;

import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.ColumnMeta;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JSONPathProcessor;
import org.neuinfo.foundry.common.util.JSONPathProcessor2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 2/28/17.
 */
public class CursorUtils {


    public static Map<String, List<JSONObject>> prepHashIndex(String resetJsonPath, List<JSONObject> records) throws Exception {
        Map<String, List<JSONObject>> index = new HashMap<String, List<JSONObject>>();
        JSONPathProcessor2 processor = new JSONPathProcessor2();
        JSONPathProcessor.Path path = processor.compile(resetJsonPath);
        for (JSONObject json : records) {
            List<JSONPathProcessor2.JPNode> jpNodes = processor.find(path, json);
            if (jpNodes != null && jpNodes.size() == 1) {
                  String value = jpNodes.get(0).getValue();
                List<JSONObject> list = index.get(value);
                if (list == null) {
                    list = new ArrayList<JSONObject>(1);
                    index.put(value, list);
                }
                list.add(json);
            }
        }
        return index;
    }

    public static List<JSONObject> filter(String refValue, String resetJsonPath,
                                          List<JSONObject> records) throws Exception {
        if (resetJsonPath == null) {
            return records;
        }
        JSONPathProcessor2 processor = new JSONPathProcessor2();
        JSONPathProcessor.Path path = processor.compile(resetJsonPath);
        List<JSONObject> list = new ArrayList<JSONObject>();
        for (JSONObject json : records) {
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

    public static JSONObject handleExternalColumnNames(JSONObject json, List<ColumnMeta> cmList) {
        if (cmList != null) {
            JSONArray jsonArray = findSingleArray(json);
            if (jsonArray != null) {
                Assertion.assertTrue(jsonArray.length() == cmList.size());
                JSONObject record = new JSONObject();
                for(int i= 0; i < jsonArray.length(); i++) {
                    Object o = jsonArray.get(i);
                    if (o instanceof JSONObject) {
                        JSONObject js = (JSONObject) o;
                        if (js.has("_$")) {
                            record.put(cmList.get(i).getName(), js.getString("_$"));
                        } else {
                            record.put(cmList.get(i).getName(), "");
                        }
                    } else {
                        record.put(cmList.get(i).getName(), o.toString());
                    }
                }
                json = record;
            }
        }
        return json;
    }

    static JSONArray findSingleArray(JSONObject json) {
        Object theObj = null;
        for(String key : json.keySet()) {
            Object o = json.get(key);
            if (o instanceof JSONObject || o instanceof JSONArray) {
                theObj = o;
                break;
            }
        }
        if (theObj == null) {
            return null;
        }
        Object o = theObj;
        while( o != null && o instanceof JSONObject) {
            JSONObject jsonObject = (JSONObject) o;
            if (jsonObject.keySet().size() != 1) {
                return null;
            }
            o = jsonObject.get(jsonObject.keySet().iterator().next());
        }
        if (o != null && o instanceof JSONArray) {
            return (JSONArray) o;
        }
        return null;
    }

}
