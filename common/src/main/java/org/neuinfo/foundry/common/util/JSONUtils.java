package org.neuinfo.foundry.common.util;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.JsonNode;
import org.neuinfo.foundry.common.transform.JSONPathUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by bozyurt on 4/8/14.
 */
public class JSONUtils {

    /**
     * given a selector path from root (no arrays) finds a nested element
     *
     * @param dbo
     * @param selectorPath
     * @return
     */
    public static Object findNested(DBObject dbo, String selectorPath) {
        String[] toks = selectorPath.split("\\.");
        DBObject p = dbo;
        for (String tok : toks) {
            final Object o = p.get(tok);
            if (o == null) {
                return null;
            }
            if (o instanceof DBObject) {
                p = (DBObject) o;
            } else {
                break;
            }
        }
        Object foundObj = p.get(toks[toks.length - 1]);
        return foundObj;
    }

    public static DBObject encode(JSONArray a) {
        return encode(a, false);
    }

    public static DBObject encode(JSONObject o) {
        return encode(o, false);
    }

    public static DBObject encode(JSONArray a, boolean escape$) {
        BasicDBList result = new BasicDBList();
        try {
            for (int i = 0; i < a.length(); ++i) {
                Object o = a.get(i);
                if (o instanceof JSONObject) {
                    result.add(encode((JSONObject) o, escape$));
                } else if (o instanceof JSONArray) {
                    result.add(encode((JSONArray) o));
                } else {
                    result.add(o);
                }
            }
            return result;
        } catch (JSONException je) {
            return null;
        }
    }

    public static DBObject encode(JSONObject o, boolean escape$) {
        BasicDBObject result = new BasicDBObject();
        try {
            Iterator i = o.keys();
            while (i.hasNext()) {
                String k = (String) i.next();
                Object v = o.get(k);
                // to make MongoDB happy
                if (escape$ && k.startsWith("$")) {
                    k = "_" + k;
                }
                if (k.indexOf('.') != -1) {
                    // mongo does not allow period in a key
                    k = k.replaceAll("\\.","");
                }
                if (v instanceof JSONArray) {
                    result.put(k, encode((JSONArray) v));
                } else if (v instanceof JSONObject) {
                    result.put(k, encode((JSONObject) v, escape$));
                } else {
                    if (v == null || v.toString().equalsIgnoreCase("null")) {
                        v = "";
                    }
                    result.put(k, v);
                }
            }
            return result;
        } catch (JSONException je) {
            return null;
        }
    }


    /**
     * Given a MongoDB <code>BasicDBObject</code> convert it to a JSON object unescaping <code>_$</code> back to <code>$</code>
     * if <code>unEscape$</code> is true.
     *
     * @param dbObject
     * @param unEscape$
     * @return
     */
    public static JSONObject toJSON(BasicDBObject dbObject, boolean unEscape$) throws JSONException {
        final String jsonStr = dbObject.toString();
        JSONObject json = new JSONObject(jsonStr);
        if (unEscape$) {
            unEscapeJson(json);
        }
        return json;
    }


    public static JSONArray toJSONArray(BasicDBList dbList) {
        String jsonStr = dbList.toString();
        JSONArray jsArr = new JSONArray(jsonStr);
        return jsArr;
    }

    public static void escapeJson(JSONObject parent) throws JSONException {
        Set<String> keys = new HashSet<String>(parent.keySet());
        final Iterator<String> it = keys.iterator();
        while (it.hasNext()) {
            String key = it.next();
            final Object v = parent.get(key);
            if (key.startsWith("$")) {
                parent.remove(key);
                String newKey = "_" + key;
                parent.put(newKey, v);
            }
            if (v instanceof JSONObject) {
                escapeJson((JSONObject) v);
            } else if (v instanceof JSONArray) {
                escapeJson((JSONArray) v);
            }
        }
    }

    public static void escapeJson(JSONArray parent) throws JSONException {
        int len = parent.length();
        for (int i = 0; i < len; i++) {
            final Object o = parent.get(i);
            if (o instanceof JSONObject) {
                escapeJson((JSONObject) o);
            } else if (o instanceof JSONArray) {
                escapeJson((JSONArray) o);
            }
        }
    }

    public static void unEscapeJson(JSONObject parent) throws JSONException {
        Set<String> keys = new HashSet<String>(parent.keySet());
        final Iterator<String> it = keys.iterator();
        while (it.hasNext()) {
            String key = it.next();
            final Object v = parent.get(key);
            if (key.startsWith("_$")) {
                parent.remove(key);
                String newKey = key.substring(1);
                parent.put(newKey, v);
            }
            if (v instanceof JSONObject) {
                unEscapeJson((JSONObject) v);
            } else if (v instanceof JSONArray) {
                unEscapeJson((JSONArray) v);
            }
        }
    }

    private static void unEscapeJson(JSONArray parent) throws JSONException {
        int len = parent.length();
        for (int i = 0; i < len; i++) {
            final Object o = parent.get(i);
            if (o instanceof JSONObject) {
                unEscapeJson((JSONObject) o);
            } else if (o instanceof JSONArray) {
                unEscapeJson((JSONArray) o);
            }
        }
    }

    public static void add2JSON(JSONObject json, String name, String value) {
        if (value != null) {
            json.put(name, value);
        } else {
            json.put(name, "");
        }
    }

    public static JSONObject loadFromFile(String jsonFilePath) throws IOException {
        final String jsonStr = Utils.loadAsString(jsonFilePath);
        return new JSONObject(jsonStr);
    }

    public static boolean isEqual(JSONObject js1, JSONObject js2) {
        if (js1 == js2) {
            return true;
        }
        if (js1 == null || js2 == null) {
            return false;
        }
        return js1.similar(js2);
    }

    public static String toBsonDate(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        return sdf.format(date);
    }

    public static Date fromBsonDate(String bsonDateStr) throws ParseException {
         SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        return sdf.parse(bsonDateStr);
    }


    public static JsonNode toTraversableJSONDOM(JSONObject json) {
        JsonNode root = new JsonNode(json, "", null, false);
        for (String key : json.keySet()) {
            toTraversableJSONDOM(root, key, json.get(key), false, -1);
        }
        return root;
    }

    static void toTraversableJSONDOM(JsonNode parentJSNode, String name, Object json, boolean array, int arrIdx) {
        if (json instanceof JSONObject) {
            JsonNode jsonNode = parentJSNode.addChild(json, name, array, arrIdx);
            jsonNode.setArray(array);
            JSONObject js = (JSONObject) json;
            for (String key : js.keySet()) {
                toTraversableJSONDOM(jsonNode, key, js.get(key), false, arrIdx);
            }
        } else if (json instanceof JSONArray) {
            JSONArray jsArr = (JSONArray) json;
            for (int i = 0; i < jsArr.length(); i++) {
                Object o = jsArr.get(i);
                toTraversableJSONDOM(parentJSNode, name, o, true, i);
            }
        } else {
            parentJSNode.addChild(json, name, array, arrIdx);
        }
    }

    public static List<JsonNode> findAllLeafNodes(JsonNode root) {
        Set<JsonNode> leafNodes = new LinkedHashSet<JsonNode>();
        findAllLeafNodes(root, leafNodes);
        return new ArrayList<JsonNode>(leafNodes);
    }

    static void findAllLeafNodes(JsonNode parent, Set<JsonNode> leafNodes) {
        if (!parent.hasChildren()) {
            leafNodes.add(parent);
        } else {
            for (JsonNode child : parent.getChildren()) {
                findAllLeafNodes(child, leafNodes);
            }
        }
    }

    public static void createOrSetFullPath(JSONObject rootJson, String leafJsonPath, Object value) {
        String[] tokens = leafJsonPath.split("\\.");
        Object parent = rootJson;
        for(int i = 0; i < tokens.length - 1; i++) {
            String token = tokens[i];
            boolean isArray = token.indexOf('[') != -1;
            String name = extractName(token);
            if (has(parent, name)) {
                Object o = ((JSONObject) parent).get(name);
                if (o instanceof JSONObject) {
                    parent = o;
                } else {
                    JSONArray jsArr = (JSONArray) o;
                    int assignedIdx = JSONPathUtils.getAssignedArrayIndex(token);
                    Assertion.assertTrue(assignedIdx != -1);
                    parent =  JSONPathUtils.createOrSetArrayElem(assignedIdx, jsArr, false, null);
                }
            } else {
                if (parent instanceof JSONObject) {
                    JSONObject parentJSON = (JSONObject) parent;
                    if (isArray) {
                        JSONArray jsArr = new JSONArray();
                        int assignedIdx = JSONPathUtils.getAssignedArrayIndex(token);
                        Assertion.assertTrue(assignedIdx != -1);
                        parent =  JSONPathUtils.createOrSetArrayElem(assignedIdx, jsArr, false, null);
                        parentJSON.put(name, jsArr);

                    } else {
                        JSONObject child = new JSONObject();
                        parent = child;
                        parentJSON.put(name, child);
                    }
                }
            }
        }
        String lastToken = tokens[tokens.length - 1];
        boolean isArray = lastToken.indexOf('[') != -1;
        String name = extractName(lastToken);
        if (has(parent, name)) {
            Object o = ((JSONObject) parent).get(name);
            if (o instanceof JSONArray) {
                JSONArray jsArr = (JSONArray) o;
                int assignedIdx = JSONPathUtils.getAssignedArrayIndex(lastToken);
                Assertion.assertTrue(assignedIdx != -1);
                jsArr.put(assignedIdx, value);
            } else {
                throw new RuntimeException("Should not happen!");
            }
        } else {
            if (parent instanceof JSONObject) {
                JSONObject parentJSON = (JSONObject) parent;
                if (isArray) {
                    JSONArray arr = new JSONArray();
                    int assignedIdx = JSONPathUtils.getAssignedArrayIndex(lastToken);
                    Assertion.assertTrue(assignedIdx != -1);
                    arr.put(assignedIdx, value);
                    parentJSON.put(name, arr);
                } else {
                    parentJSON.put(name, value);
                }
            } else {
                JSONArray parentArr = (JSONArray) parent;
                int assignedIdx = JSONPathUtils.getAssignedArrayIndex(lastToken);
                Assertion.assertTrue(assignedIdx != -1);
                parentArr.put(assignedIdx,value);
            }
        }
    }



    public static boolean has(Object o, String key) {
        if (o instanceof JSONObject) {
            return ((JSONObject) o).has(key);
        }
        return false;
    }
    public static String extractName(String token) {
        int idx = token.indexOf('[');
        return (idx == -1) ? token : token.substring(0, idx);
    }


}
