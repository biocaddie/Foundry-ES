package org.neuinfo.foundry.common.util;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;

/**
 * Converts a JSON object to JSTree tree view JSON format
 * Created by bozyurt on 8/21/15.
 */
public class JSTreeJSONGenerator {
    int counter = 0;

    public JSONObject toJSTree(JSONObject origJson) {
        JSONObject js = new JSONObject();
        js.put("id", ++counter);
        js.put("text", "");
        toJSTree("", origJson, js);
        return js;
    }

    void toJSTree(String name, Object parent, JSONObject jstParent) {
        // JSONObject jst = new JSONObject();
        // jst.put("id", ++counter);
        // jst.put("text", name);
        // addChild(jstParent, jst);
        if (parent instanceof JSONObject) {
            JSONObject p = (JSONObject) parent;

            JSONObject jst = jstParent;
            if (!name.equals("")) {
                jst = new JSONObject();
                jst.put("id", ++counter);
                jst.put("text", name);
                addChild(jstParent, jst);
            }
            for (String key : p.keySet()) {
                Object o = p.get(key);
                toJSTree(key, o, jst);
                /*
                if (o instanceof JSONObject) {
                    toJSTree(key, (JSONObject) o, jst);
                } else if (o instanceof JSONArray) {
                    JSONObject ajst = new JSONObject();
                    ajst.put("id", ++counter);
                    ajst.put("text", key + "[]");
                    addChild(jst, ajst);
                    JSONArray jsArr = (JSONArray) o;
                    for (int i = 0; i < jsArr.length(); i++) {

                    }

                } else {
                    JSONObject ajst = new JSONObject();
                    ajst.put("id", ++counter);
                    ajst.put("text", key + " : " + o.toString());
                    addChild(jst, ajst);
                }
                */
            }
        } else if (parent instanceof JSONArray) {
            JSONArray jsArr = (JSONArray) parent;
            JSONObject jst = new JSONObject();
            jst.put("id", ++counter);
            jst.put("text", name + "[]");
            addChild(jstParent, jst);
            for (int i = 0; i < jsArr.length(); i++) {
                toJSTree(""+i, jsArr.get(i), jst);
            }

        } else {
            JSONObject jst = new JSONObject();
            jst.put("id", ++counter);
            jst.put("text", name + " : " + parent.toString());
            addChild(jstParent, jst);
        }
    }

    static void addChild(JSONObject parentJST, JSONObject jst) {
        JSONArray jsArr = null;
        if (parentJST.has("children")) {
            jsArr = parentJST.getJSONArray("children");
        } else {
            jsArr = new JSONArray();
            parentJST.put("children", jsArr);
        }
        jsArr.put(jst);
    }

    public static void main(String[] args) throws IOException {
        String HOME_DIR = System.getProperty("user.home");
        String jsonFile = HOME_DIR + "/dev/java/Foundry-Data/SampleData/LINCS/cells/50001.json";
        jsonFile = "/var/data/test/test.json";

        String s = Utils.loadAsString(jsonFile);
        JSONObject json = new JSONObject(s);
        System.out.println(json.toString(2));
        System.out.println("============================");
        JSTreeJSONGenerator gen = new JSTreeJSONGenerator();

        JSONObject jsTreeJSON = gen.toJSTree(json);
        System.out.println(jsTreeJSON.toString(2));
        Utils.saveText(jsTreeJSON.toString(2), "/tmp/root.json");


    }

}
