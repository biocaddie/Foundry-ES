package org.neuinfo.foundry.common.transform;

import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.JSONPathProcessor2;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.neuinfo.foundry.common.util.Utils;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bozyurt on 4/30/15.
 */
public class JSONPathUtils {

    public static class Parent {
        final JSONObject parent;
        final int idx;

        public Parent(JSONObject parent, int idx) {
            this.parent = parent;
            this.idx = idx;
        }

        public JSONObject getParent() {
            return parent;
        }

        public int getIdx() {
            return idx;
        }
    }

    static Object createOrAppendArrayElem(JSONArray parentArr, boolean isNextAnArray) {
        int len = parentArr.length();
        Assertion.assertTrue(!isNextAnArray);
        JSONObject child = new JSONObject();
        parentArr.put(len, child);
        return child;
    }

    public static Object createOrSetArrayElem(int elIdx, JSONArray parentArr, boolean isNextAnArray, String nextName) {
        int len = parentArr.length();
        if (len > elIdx) {
            Object o = parentArr.get(elIdx);
            if (o == JSONObject.NULL) {
                JSONObject child = new JSONObject();
                parentArr.put(elIdx, child);
                return child;
            }
            return o;
        } else {
            // create
            if (isNextAnArray) {
                JSONArray arr = new JSONArray();
                JSONObject child = new JSONObject();
                child.put(nextName, arr);
                parentArr.put(elIdx, child);
                if (elIdx > 0) {
                    for (int i = 0; i < elIdx; i++) {
                        if (parentArr.isNull(i)) {
                            parentArr.put(i, new JSONObject());
                        }
                    }
                }
                return child;
            } else {
                JSONObject child = new JSONObject();
                parentArr.put(elIdx, child);
                return child;
            }
        }
    }

    public static int getAssignedArrayIndex(String token) {
        if (token.endsWith("[]") || token.indexOf('[') == -1) {
            return -1;
        }
        int idx = token.indexOf('[');
        StringBuilder buf = new StringBuilder();
        for (int i = idx + 1; i < token.length(); i++) {
            char c = token.charAt(i);
            if (c == ']') {
                break;
            }
            buf.append(c);
        }
        return Utils.getIntValue(buf.toString(), -1);
    }

    public static String extractName(String token) {
        int idx = token.indexOf('[');
        return (idx == -1) ? token : token.substring(0, idx);
    }

    static void createOrSetFullPath(JSONObject rootJson, String jsonPath, JSONPathProcessor2.JPNode value) {
        String[] tokens = jsonPath.split("\\.");

        int arrayIdx = 0;
        Object parent = rootJson;
        for (int i = 0; i < tokens.length - 1; i++) {
            String token = tokens[i].trim();
            boolean isArray = token.indexOf('[') != -1;
            String name = extractName(token);

            if (JSONUtils.has(parent, name)) {
                Object o = ((JSONObject) parent).get(name);
                if (o instanceof JSONObject) {
                    parent = o;
                } else {
                    JSONArray jsArr = (JSONArray) o;
                    int assignedIdx = getAssignedArrayIndex(token);
                    if (assignedIdx == -1 && !value.hasArrays()) {
                        parent = createOrAppendArrayElem(jsArr, isArrayToken(tokens[i + 1]));
                    } else {
                        Assertion.assertTrue(assignedIdx != -1 || value.hasArrays());
                        int elIdx = assignedIdx;
                        elIdx = getElIdx(value, arrayIdx, jsArr, assignedIdx, elIdx);
                        parent = createOrSetArrayElem(elIdx, jsArr, isArrayToken(tokens[i + 1]),
                                extractName(tokens[i + 1]));
                        if (assignedIdx == -1) {
                            arrayIdx++;
                        }
                    }
                }
            } else {
                //
                if (parent instanceof JSONObject) {
                    JSONObject parentJSON = (JSONObject) parent;
                    if (isArray) {
                        JSONArray arr = new JSONArray();
                        int assignedIdx = getAssignedArrayIndex(token);
                        if (assignedIdx == -1 && !value.hasArrays()) {
                            parent = createOrAppendArrayElem(arr, isArrayToken(tokens[i + 1]));
                            parentJSON.put(name, arr);
                        } else {
                            Assertion.assertTrue(assignedIdx != -1 || value.hasArrays());
                            int elIdx = assignedIdx;
                            elIdx = getElIdx(value, arrayIdx, arr, assignedIdx, elIdx);
                            parent = createOrSetArrayElem(elIdx, arr, isArrayToken(tokens[i + 1]),
                                    extractName(tokens[i + 1]));
                            parentJSON.put(name, arr);
                            if (assignedIdx == -1) {
                                arrayIdx++;
                            }
                        }
                    } else {
                        JSONObject child = new JSONObject();
                        parent = child;
                        parentJSON.put(name, child);
                    }
                } else {
                    JSONArray parentArr = (JSONArray) parent;
                    Assertion.assertTrue(value.hasArrays());
                    int elIdx = value.getIndices()[arrayIdx];
                    parent = createOrSetArrayElem(elIdx, parentArr, isArray, name);
                    arrayIdx++;
                }
            }
        }
        String lastToken = tokens[tokens.length - 1];

        boolean isArray = lastToken.indexOf('[') != -1;
        String name = extractName(lastToken);
        if (JSONUtils.has(parent, name)) {
            Object o = ((JSONObject) parent).get(name);
            if (o instanceof JSONArray) {
                JSONArray jsArr = (JSONArray) o;
                int assignedIdx = getAssignedArrayIndex(lastToken);
                if (assignedIdx == -1 && !value.hasArrays()) {
                    // no index info so add to the end of the array
                    int len = jsArr.length();
                    jsArr.put(len, value.getValue());
                } else {
                    Assertion.assertTrue(assignedIdx != -1 || value.hasArrays());
                    int elIdx = assignedIdx;
                    elIdx = getElIdx(value, arrayIdx, jsArr, assignedIdx, elIdx);
                    jsArr.put(elIdx, value.getValue());
                }
            } else {
                System.err.println("Should not happen! Field '" + name + "' already exists on parent:" + tokens[tokens.length - 2]);
                // throw new RuntimeException("Should not happen! Field '" + name + "' already exists on parent:" + tokens[tokens.length - 2]);
            }
        } else {
            if (parent instanceof JSONObject) {
                JSONObject parentJSON = (JSONObject) parent;
                if (isArray) {
                    JSONArray arr = new JSONArray();
                    int assignedIdx = getAssignedArrayIndex(lastToken);
                    if (assignedIdx == -1 && !value.hasArrays()) {
                        // no index info so add to the end of the array
                        int len = arr.length();
                        arr.put(len, value.getValue());
                        parentJSON.put(name, arr);
                    } else {
                        Assertion.assertTrue(assignedIdx != -1 || value.hasArrays());
                        int elIdx = assignedIdx;
                        elIdx = getElIdx(value, arrayIdx, arr, assignedIdx, elIdx);
                        arr.put(elIdx, value.getValue());
                        parentJSON.put(name, arr);
                    }
                } else {
                    parentJSON.put(name, value.getValue());
                }
            } else {
                if (parent == JSONObject.NULL) {
                    System.out.println(parent);
                }
                JSONArray parentArr = (JSONArray) parent;
                int assignedIdx = getAssignedArrayIndex(lastToken);
                Assertion.assertTrue(assignedIdx != -1 || value.hasArrays());
                int elIdx = assignedIdx;
                if (assignedIdx == -1) {
                    elIdx = value.getIndices()[arrayIdx];
                }
                parentArr.put(elIdx, value.getValue());
            }
        }

    }

    private static int getElIdx(JSONPathProcessor2.JPNode value, int arrayIdx, JSONArray jsArr, int assignedIdx, int elIdx) {
        if (elIdx < 0 && value.hasArrays()) {
            if (value.getIndices().length <= arrayIdx) {
                // add to the end of the array
                elIdx = jsArr.length();
            } else {
                if (assignedIdx == -1) {
                    elIdx = value.getIndices()[arrayIdx];
                }
            }
        }
        return elIdx;
    }


    static boolean isArrayToken(String token) {
        return token.endsWith("[]");
    }

    public static void setJSONField2(JSONObject json, String jsonPath, JSONPathProcessor2.JPNode value) {
        try {
            createOrSetFullPath(json, jsonPath, value);
        } catch (RuntimeException t) {
            System.err.println("Error in:" + jsonPath);
            throw t;
        }
    }

    public static Parent setJSONField(JSONObject json, String jsonPath, String value, Parent parentObj) {
        if (jsonPath.indexOf('.') == -1) {
            createOrSet(json, jsonPath, value);
            return new Parent(json, -1);
        } else {
            JSONObject parent = json;
            String[] tokens = jsonPath.split("\\.");
            if (parentObj != null) {
                createOrSet(parentObj.getParent(), tokens[tokens.length - 1].trim(), value);
                return parentObj;
            }

            Parent p = null;
            for (int i = 0; i < tokens.length - 1; i++) {
                String token = tokens[i].trim();
                boolean isArray = token.endsWith("[]");
                if (!isArray) {
                    if (!parent.has(token)) {
                        JSONObject js = new JSONObject();
                        parent.put(token, js);
                        parent = js;
                    } else {
                        parent = parent.getJSONObject(token);
                    }
                } else {
                    String name = token.substring(0, token.length() - 2);
                    JSONArray jsArr;
                    if (!parent.has(name)) {
                        jsArr = new JSONArray();
                        parent.put(name, jsArr);
                        JSONObject js = new JSONObject();
                        jsArr.put(js);
                        parent = js;
                        p = new Parent(parent, jsArr.length() - 1);
                    } else {
                        jsArr = parent.getJSONArray(name);
                        JSONObject js = new JSONObject();
                        jsArr.put(js);
                        parent = js;
                        p = new Parent(parent, jsArr.length() - 1);
                    }
                }
            }
            createOrSet(parent, tokens[tokens.length - 1].trim(), value);
            return p;
        }
    }

    static Pattern idxPattern = Pattern.compile("\\[(\\d+)\\]$");

    public static boolean hasArrayIndex(String token) {
        Matcher matcher = idxPattern.matcher(token);
        return matcher.find();
    }

    public static int getArrayIndex(String token) {
        Matcher matcher = idxPattern.matcher(token);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        return -1;
    }

    public static Parent setJSONField2(JSONObject json, String jsonPath, String value, Parent parentObj) {
        if (jsonPath.indexOf('.') == -1) {
            createOrSet(json, jsonPath, value);
            return new Parent(json, -1);
        } else {
            JSONObject parent = json;
            String[] tokens = jsonPath.split("\\.");
            if (parentObj != null) {
                createOrSet(parentObj.getParent(), tokens[tokens.length - 1].trim(), value);
                return parentObj;
            }

            Parent p = null;
            for (int i = 0; i < tokens.length - 1; i++) {
                String token = tokens[i].trim();
                boolean hasIndex = hasArrayIndex(token);
                boolean isArray = token.endsWith("[]") || hasIndex;
                if (!isArray) {
                    if (!parent.has(token)) {
                        JSONObject js = new JSONObject();
                        parent.put(token, js);
                        parent = js;
                    } else {
                        parent = parent.getJSONObject(token);
                    }
                } else {
                    int idx = token.lastIndexOf('[');
                    String name = token.substring(0, idx);
                    JSONArray jsArr;
                    if (!parent.has(name)) {
                        jsArr = new JSONArray();
                        parent.put(name, jsArr);
                        JSONObject js = new JSONObject();
                        jsArr.put(js);
                        parent = js;
                        p = new Parent(parent, jsArr.length() - 1);
                    } else {
                        jsArr = parent.getJSONArray(name);
                        if (hasIndex) {
                            int arrIdx = getArrayIndex(token);
                            JSONObject js = jsArr.getJSONObject(arrIdx);
                            parent = js;
                            p = new Parent(parent, jsArr.length() - 1);
                        } else {
                            JSONObject js = new JSONObject();
                            jsArr.put(js);
                            parent = js;
                            p = new Parent(parent, jsArr.length() - 1);
                        }
                    }
                }
            }
            createOrSet(parent, tokens[tokens.length - 1].trim(), value);
            return p;
        }
    }

    public static void createOrSet(JSONObject json, String jsonPath, String value) {
        boolean isArray = jsonPath.endsWith("[]");
        if (!isArray) {
            json.put(jsonPath, value);
        } else {
            String name = jsonPath.substring(0, jsonPath.length() - 2);
            JSONArray jsArr;
            if (!json.has(name)) {
                jsArr = new JSONArray();
                json.put(name, jsArr);
            } else {
                jsArr = json.getJSONArray(name);
            }
            jsArr.put(value);
        }
    }

    public static void main(String[] args) throws Exception {
        String HOME_DIR = System.getProperty("user.home");
        String jsonFile = HOME_DIR + "/dev/java/Foundry-Data/SampleData/SRA/sra_sample_record_138.json";
        String jsonPath = "$.'SRA'.'SAMPLE_SET'.'SAMPLE'[*].'SAMPLE_ATTRIBUTES'.'SAMPLE_ATTRIBUTE'[*].'VALUE'.'_$'";

        JSONObject json = JSONUtils.loadFromFile(jsonFile);

        JSONPathProcessor2 processor = new JSONPathProcessor2();
        List<JSONPathProcessor2.JPNode> list = processor.find(jsonPath, json);
        for (Object o : list) {
            System.out.println(o);
        }

        String toPath = "SRA.SAMPLE_SET.SAMPLE[].SAMPLE_ATTRIBUTES.SAMPLE_ATTRIBUTE[].VALUE";
        JSONObject destJSON = new JSONObject();
        for (JSONPathProcessor2.JPNode value : list) {
            JSONPathUtils.setJSONField2(destJSON, toPath, value);
            // break;
        }
        System.out.println("===========================");
        System.out.println(destJSON.toString(2));
    }
}
