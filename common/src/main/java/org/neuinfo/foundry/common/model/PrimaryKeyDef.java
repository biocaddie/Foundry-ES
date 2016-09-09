package org.neuinfo.foundry.common.model;

import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JSONPathProcessor;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bozyurt on 10/30/14.
 */
public class PrimaryKeyDef {
    List<String> fields;
    List<String> delimiters;
    String method;

    public PrimaryKeyDef() {
    }

    public PrimaryKeyDef(List<String> fields, List<String> delimiters, String method) {
        this.fields = fields;
        this.delimiters = delimiters;
        this.method = method;
    }

    public List<String> getFields() {
        return fields;
    }

    public List<String> getDelimiters() {
        return delimiters;
    }

    public String getMethod() {
        return method;
    }

    public JSONObject toJSON() {
        JSONObject js = new JSONObject();
        JSONArray jsArr = new JSONArray();
        for (String field : fields) {
            jsArr.put(field);
        }
        js.put("fields", jsArr);
        jsArr = new JSONArray();
        for (String delimiter : delimiters) {
            jsArr.put(delimiter);
        }
        js.put("delimiter", jsArr);
        js.put("method", method);
        return js;
    }

    public static PrimaryKeyDef fromJSON(JSONObject json) {
        PrimaryKeyDef pk = new PrimaryKeyDef();
        String method = json.getString("method");
        JSONArray jsArr = json.getJSONArray("fields");
        pk.fields = new ArrayList<String>(jsArr.length());
        for (int i = 0; i < jsArr.length(); i++) {
            pk.fields.add(jsArr.getString(i));
        }
        jsArr = json.getJSONArray("delimiter");
        pk.delimiters = new ArrayList<String>(jsArr.length());
        for (int i = 0; i < jsArr.length(); i++) {
            pk.delimiters.add(jsArr.getString(i));
        }
        pk.method = method;
        return pk;
    }

    public String prepPrimaryKey(JSONObject payload) throws Exception {
        StringBuilder sb = new StringBuilder();
        JSONPathProcessor processor = new JSONPathProcessor();
        int len = fields.size();
        if (this.delimiters.size() == 1) {
            String delim = delimiters.get(0);
            for (int i = 0; i < len; i++) {
                String field = fields.get(i);
                String jsPath = toJSONPath(field);
                List<Object> objects = processor.find(jsPath, payload);
                //Assertion.assertTrue(objects.size() == 1);
                if (!objects.isEmpty()) {
                    Object o = objects.get(0);
                    sb.append(o.toString());
                }
                if ((i + 1) < len) {
                    sb.append(delim);
                }
            }
        } else {
            throw new Exception("Multiple delimiters are not supported yet!");
        }

        return sb.toString();
    }

    public static String toJSONPath(String field) {
        if (field.startsWith("$.")) {
            return field;
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append("$.'").append(field).append("'");
            return sb.toString();
        }
    }
}
