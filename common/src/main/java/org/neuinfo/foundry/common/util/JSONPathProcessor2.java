package org.neuinfo.foundry.common.util;

import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.JSONPathProcessor.Node;
import org.neuinfo.foundry.common.util.JSONPathProcessor.Path;

import java.util.*;

/**
 * Created by bozyurt on 2/11/16.
 */
public class JSONPathProcessor2 {

    public static class JPNode {
        Object payload;
        int[] indices;
        String[] names;

        public JPNode(Object payload) {
            this.payload = payload;
        }

        public JPNode(JPNode other) {
            this.payload = other.payload;
            this.indices = other.indices;
            this.names = other.names;
        }

        public void setPayload(Object payload) {
            this.payload = payload;
        }

        public Object getPayload() {
            return payload;
        }

        public int getIndexAt(int arrayIdx) {
            return -1;
        }

        public int[] getIndices() {
            return indices;
        }


        public String[] getNames() {
            return names;
        }

        public boolean hasArrays() {
            return indices != null;
        }

        public String getValue() {
            return payload.toString();
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("JPNode::{");
            sb.append("payload=").append(payload);
            if (indices != null) {
                sb.append(", indices=").append(Arrays.toString(indices));
                sb.append(", names=").append(Arrays.toString(names));
            }
            sb.append('}');
            return sb.toString();
        }
    }

    public static class Accumulator {
        int order;
        String name;
        int counter = 0;

        public Accumulator(String name, int order) {
            this.name = name;
            this.order = order;
        }

        public void incr() {
            counter++;
        }

        public String getName() {
            return name;
        }

        public int getCounter() {
            return counter;
        }
    }

    public static class AccumulatorGroup {
        int order = 0;
        Map<String, Accumulator> accumMap = new LinkedHashMap<String, Accumulator>(7);

        public void updateAccum(Node node) {
            String key = node.getKey();
            Accumulator accumulator = accumMap.get(key);
            if (accumulator == null) {
                accumulator = new Accumulator(node.name, order);
                accumMap.put(key, accumulator);
                order++;
            } else {
                // check if there are downstream accumulators and reset them
                if (accumMap.size() > 1) {
                    int i = 0;
                    for (Accumulator ac : accumMap.values()) {
                        if (i > accumulator.order) {
                            ac.counter = -1;
                        }
                        i++;
                    }
                }
                accumulator.incr();
            }
        }

        public void setArrayInfo(JPNode jpNode) {
            if (!accumMap.isEmpty()) {
                int len = accumMap.size();
                int[] indices = new int[len];
                String[] names = new String[len];
                int i = 0;
                for (Accumulator ac : accumMap.values()) {
                    indices[i] = ac.getCounter();
                    names[i] = ac.getName();
                    i++;
                }
                jpNode.indices = indices;
                jpNode.names = names;
            }
        }
    }


    public List<JPNode> find(String jsonPathExpr, JSONObject json) throws Exception {
        JsonPathParser parser = new JsonPathParser(jsonPathExpr);

        JSONPathProcessor.Path path = parser.parse();
        List<JPNode> list = new ArrayList<JPNode>(5);
        AccumulatorGroup ag = new AccumulatorGroup();
        find(path.start, list, json, null, ag);
        return list;
    }

    public JSONPathProcessor.Path compile(String jsonPathExpr) throws Exception {
        JsonPathParser parser = new JsonPathParser(jsonPathExpr);
        return parser.parse();
    }

    public List<JPNode> find(JSONPathProcessor.Path path, JSONObject json) {
        AccumulatorGroup ag = new AccumulatorGroup();
        List<JPNode> list = new ArrayList<JPNode>(5);
        find(path.start, list, json, null, ag);
        return list;
    }

    public List<JPNode> find(String jsonPathExpr, JSONObject json, List<String> pathToMatch) throws Exception {
        JsonPathParser parser = new JsonPathParser(jsonPathExpr);

        Path path = parser.parse();
        AccumulatorGroup ag = new AccumulatorGroup();
        List<JPNode> list = new ArrayList<JPNode>(5);
        find(path.start, list, json, pathToMatch, ag);
        return list;
    }

    private void findMatching(Node node, Object parent, List<Object> list) {
        if (parent instanceof JSONObject) {
            JSONObject p = (JSONObject) parent;
            if (p.has(node.name)) {
                if (!node.hasPredicate()) {
                    list.add(p.get(node.name));
                } else {
                    Object obj = p.get(node.name);
                    if (obj instanceof JSONObject) {
                        if (node.testPredicate((JSONObject) obj)) {
                            list.add(obj);
                        }
                    } else if (obj instanceof JSONArray) {
                        JSONArray jsArr = (JSONArray) obj;
                        int len = jsArr.length();
                        for (int i = 0; i < len; i++) {
                            Object o = jsArr.get(i);
                            if (o instanceof JSONObject) {
                                if (node.testPredicate((JSONObject) o)) {
                                    list.add(o);
                                }
                            }
                        }
                    }
                }
            }
            final Iterator<String> keys = p.keys();
            while (keys.hasNext()) {
                String key = keys.next();
                Object child = p.get(key);
                if (child instanceof JSONObject || child instanceof JSONArray) {
                    findMatching(node, child, list);
                }
            }
        } else if (parent instanceof JSONArray) {
            JSONArray jsArr = (JSONArray) parent;
            for (int i = 0; i < jsArr.length(); i++) {
                Object child = jsArr.get(i);
                if (child instanceof JSONObject || child instanceof JSONArray) {
                    findMatching(node, child, list);
                }
            }
        }

    }

    private void add2Path2Match(String jsonObjName, List<String> pathToMatch) {
        if (pathToMatch == null) {
            return;
        }
        if (pathToMatch.isEmpty()) {
            pathToMatch.add(jsonObjName);
        } else {
            String last = pathToMatch.get(pathToMatch.size() - 1);
            if (!last.equals("<SENTINEL>")) {
                pathToMatch.add(jsonObjName);
            }
        }
    }

    private void find(Node node, List<JPNode> list, Object parent, List<String> pathToMatch, AccumulatorGroup ag) {
        if (node.type == Node.FROM_ROOT || node.type == Node.INNER_NODE) {
            if (parent instanceof JSONObject) {
                JSONObject p = (JSONObject) parent;
                if (!p.has(node.name)) {
                    return;
                }
                if (node.includeAll || node.idx >= 0 || node.hasPredicate()) {
                    if (node.idx >= 0) {
                        JSONArray jsonArray = p.getJSONArray(node.name);
                        if (jsonArray.length() <= node.idx) {
                            // array is too short
                            return;
                        }
                        final Object o = jsonArray.get(node.idx);
                        addOrFind(node, list, pathToMatch, ag, o);
                    } else if (node.includeAll) {
                        Object o = p.get(node.name);
                        if (o instanceof JSONArray) {
                            JSONArray jsArr = (JSONArray) o;
                            if (node.next == null) {
                                for (int i = 0; i < jsArr.length(); i++) {
                                    ag.updateAccum(node);
                                    Object child = jsArr.get(i);
                                    JPNode jpNode = new JPNode(child);
                                    ag.setArrayInfo(jpNode);
                                    list.add(jpNode);
                                }
                            } else {
                                for (int i = 0; i < jsArr.length(); i++) {
                                    ag.updateAccum(node);
                                    find(node.next, list, jsArr.get(i), pathToMatch, ag);
                                }
                            }
                        } else {
                            // FIXME
                            if (!(o instanceof JSONObject)) {
                                return;
                            }
                            JSONObject jsObj = (JSONObject) o;
                            ag.updateAccum(node);
                            addOrFind(node, list, pathToMatch, ag, jsObj);
                        }
                    } else if (node.hasPredicate()) {
                        Object o = p.get(node.name);
                        if (o instanceof JSONArray) {
                            JSONArray jsArr = (JSONArray) o;
                            if (node.next == null) {
                                for (int i = 0; i < jsArr.length(); i++) {
                                    Object child = jsArr.get(i);
                                    if (node.testPredicate((JSONObject) child)) {
                                        ag.updateAccum(node);
                                        JPNode jpNode = new JPNode(child);
                                        ag.setArrayInfo(jpNode);
                                        list.add(jpNode);
                                    }
                                }
                            } else {
                                for (int i = 0; i < jsArr.length(); i++) {
                                    Object child = jsArr.get(i);
                                    if (node.testPredicate((JSONObject) child)) {
                                        ag.updateAccum(node);
                                        find(node.next, list, child, pathToMatch, ag);
                                    }
                                }
                            }
                        } else {
                            JSONObject jsObj = (JSONObject) o;
                            if (node.next == null) {
                                if (node.testPredicate(jsObj)) {
                                    JPNode jpNode = new JPNode(jsObj);
                                    ag.setArrayInfo(jpNode);
                                    list.add(jpNode);
                                }
                            } else {
                                if (node.testPredicate(jsObj)) {
                                    find(node.next, list, jsObj, pathToMatch, ag);
                                }
                            }
                        }
                    }
                } else {
                    Object js = p.get(node.name);
                    addOrFind(node, list, pathToMatch, ag, js);
                }
            }
        } else if (node.type == Node.FROM_ANYWHERE) {
            List<Object> jsNodes = new LinkedList<Object>();
            findMatching(node, parent, jsNodes);

            if (!jsNodes.isEmpty()) {
                if (node.next == null) {
                    for (Object jsNode : jsNodes) {
                        JPNode jpNode = new JPNode(jsNode);
                        ag.setArrayInfo(jpNode);
                        list.add(jpNode);
                    }
                }
                for (Object jsNode : jsNodes) {
                    x(node.next, jsNode, list, pathToMatch, ag);
                }
            }
        }
    }

    private void addOrFind(Node node, List<JPNode> list, List<String> pathToMatch, AccumulatorGroup ag, Object o) {
        if (node.next == null) {
            JPNode jpNode = new JPNode(o);
            ag.setArrayInfo(jpNode);
            list.add(jpNode);
        } else {
            find(node.next, list, o, pathToMatch, ag);
        }
    }

    private void x(Node node, Object jsNode, List<JPNode> list, List<String> pathToMatch, AccumulatorGroup ag) {
        if (node == null) {
            return;
        }
        if (jsNode instanceof JSONObject) {
            JSONObject p = (JSONObject) jsNode;
            if (node.includeAll || node.idx >= 0) {
                if (node.idx >= 0) {
                    JSONArray jsonArray = p.getJSONArray(node.name);
                    if (jsonArray.length() >= node.idx) {
                        // array is too short
                        return;
                    }
                    final Object o = jsonArray.get(node.idx);
                    if (o instanceof JSONObject) {
                        addOrFind(node, list, pathToMatch, ag, o);

                    } else if (jsNode instanceof JSONArray) {
                        throw new RuntimeException("Not implemented condition!");
                        //TODO
                    }
                }
            } else {
                if (p.has(node.name)) {
                    Object js = p.get(node.name);
                    addOrFind(node, list, pathToMatch, ag, js);
                }
            }
        } else if (jsNode instanceof JSONArray) {
            JSONArray jsArr = (JSONArray) jsNode;
            if (node.idx >= 0) {
                final Object o = jsArr.get(node.idx);
                addOrFind(node, list, pathToMatch, ag, o);
            } else if (node.includeAll) {
                addOrFind(node, list, pathToMatch, ag, jsArr);
            }
        } else {
            JPNode jpNode = new JPNode(jsNode);
            ag.setArrayInfo(jpNode);
            list.add(jpNode);
        }
    }

    static String HOME_DIR = System.getProperty("user.home");

    public static void testParser() throws Exception {
        String jsonFile = HOME_DIR + "/dev/java/Foundry-Data/SampleData/SRA/sra_sample_record_138.json";
        String jsonPath = "$.'SRA'.'SAMPLE_SET'.'SAMPLE'[*].'SAMPLE_ATTRIBUTES'.'SAMPLE_ATTRIBUTE'[*].'VALUE'.'_$'";
        handleIt(jsonFile, jsonPath);
    }

    public static void testParser2() throws Exception {
        String jsonFile = HOME_DIR + "/dev/java/Foundry-Data/SampleData/dataverse/dataverse_sample_record_1.json";
        String jsonPath = "$.'authors'[*]";
        handleIt(jsonFile, jsonPath);
    }

    static void handleIt(String jsonFile, String jsonPath) throws Exception {
        JSONObject json = JSONUtils.loadFromFile(jsonFile);

        JSONPathProcessor2 processor = new JSONPathProcessor2();
        List<JPNode> list = processor.find(jsonPath, json);
        for (Object o : list) {
            System.out.println(o);
        }
    }

    public static void testParser3() throws Exception {
        String jsonFile = HOME_DIR + "/dev/java/Foundry-Data/SampleData/pdb/100d-noatom.json";
        String jsonPath = "$.'PDBx:datablock'.'PDBx:citation_authorCategory'.'PDBx:citation_author'[?(@.'@citation_id'='primary')].'@name'";
        handleIt(jsonFile, jsonPath);
    }

    public static void testParser4() throws Exception {
        String jsonFile = HOME_DIR + "/dev/java/Foundry-Data/SampleData/dataverse2/dataverse2_record_1.json";
        String jsonPath = "$.'latestVersion'.'metadataBlocks'.'citation'.'fields'[*].'value'[*].'otherIdValue'.'value'";
        jsonPath = "$.'latestVersion'.'metadataBlocks'.'citation'.'fields'[*].'value'[*].'keywordVocabulary'.'typeName'";
        handleIt(jsonFile, jsonPath);
    }

    public static void testParser5() throws Exception {
        String jsonFile = HOME_DIR + "/dev/java/Foundry-Data/SampleData/Bioproject/bioproject_sample.json";
        String jsonPath = "$.'Package'.'Project'.'Project'.'ProjectID'.'ArchiveID'.'@accession'";
        handleIt(jsonFile, jsonPath);
    }

    public static void testParser6() throws Exception {
        String jsonFile = HOME_DIR + "/dev/java/Foundry-Data/SampleData/KnowledgeSpace/Ion_Channel_Geneology/icg_record_1.json";
        String jsonPath = "$.'cls'[*].'cls'[*].'name'";
        handleIt(jsonFile, jsonPath);
    }


    public static void main(String[] args) throws Exception {
        testParser6();
    }


}
