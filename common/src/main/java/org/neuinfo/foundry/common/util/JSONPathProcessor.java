package org.neuinfo.foundry.common.util;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * <p>
 * Xpath like processor for JSON based on the syntax from <code>http://goessner.net/articles/JsonPath/</code>
 * </p>
 * Created by bozyurt on 5/22/14.
 */
public class JSONPathProcessor {


    public List<Object> find(String jsonPathExpr, JSONObject json) throws Exception {
        JsonPathParser parser = new JsonPathParser(jsonPathExpr);

        final Path path = parser.parse();
        List<Object> list = new ArrayList<Object>(5);
        find(path.start, list, json, null);
        return list;
    }

    public List<Object> find(String jsonPathExpr, JSONObject json, List<String> pathToMatch) throws Exception {
        JsonPathParser parser = new JsonPathParser(jsonPathExpr);

        final Path path = parser.parse();
        List<Object> list = new ArrayList<Object>(5);
        find(path.start, list, json, pathToMatch);
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

    private void find(Node node, List<Object> list, Object parent, List<String> pathToMatch) {
        if (node.type == Node.FROM_ROOT || node.type == Node.INNER_NODE) {
            if (parent instanceof JSONObject) {
                JSONObject p = (JSONObject) parent;
                if (!p.has(node.name)) {
                    return;
                }
                if (node.includeAll || node.idx >= 0) {
                    if (node.idx >= 0) {
                        final Object o = p.getJSONArray(node.name).get(node.idx);
                        if (node.next == null) {
                            list.add(o);
                        } else {
                            find(node.next, list, o, pathToMatch);
                        }
                    } else if (node.includeAll) {
                        Object o = p.get(node.name);
                        if (o instanceof JSONArray) {
                            final JSONArray jsArr = (JSONArray) o;
                            if (node.next == null) {
                                list.add(jsArr);
                            } else {
                                for (int i = 0; i < jsArr.length(); i++) {
                                    find(node.next, list, jsArr.get(i), pathToMatch);
                                }
                            }
                        } else {
                            JSONObject jsObj = (JSONObject) o;
                            if (node.next == null) {
                                list.add(jsObj);
                            } else {
                                find(node.next, list, jsObj, pathToMatch);
                            }
                        }
                    }
                } else {
                    Object js = p.get(node.name);
                    if (node.next == null) {
                        list.add(js);
                    } else {
                        find(node.next, list, js, pathToMatch);
                    }
                }
            }
        } else if (node.type == Node.FROM_ANYWHERE) {
            List<Object> jsNodes = new LinkedList<Object>();
            findMatching(node, parent, jsNodes);

            if (!jsNodes.isEmpty()) {
                if (node.next == null) {
                    for (Object jsNode : jsNodes) {
                        list.add(jsNode);
                    }
                }
                for (Object jsNode : jsNodes) {
                    x(node.next, jsNode, list, pathToMatch);
                }
            }
        }
    }

    private void x(Node node, Object jsNode, List<Object> list, List<String> pathToMatch) {
        if (node == null) {
            return;
        }
        if (jsNode instanceof JSONObject) {
            JSONObject p = (JSONObject) jsNode;
            if (node.includeAll || node.idx >= 0) {
                if (node.idx >= 0) {
                    final Object o = p.getJSONArray(node.name).get(node.idx);
                    if (o instanceof JSONObject) {
                        JSONObject js = (JSONObject) o;
                        if (node.next == null) {
                            list.add(js);
                        } else {
                            find(node.next, list, js, pathToMatch);
                        }
                    } else if (jsNode instanceof JSONArray) {
                        //TODO
                    }
                }
            } else {
                if (p.has(node.name)) {
                    Object js = p.get(node.name);
                    if (node.next == null) {
                        list.add(js);
                    } else {
                        find(node.next, list, js, pathToMatch);
                    }
                }
            }
        } else if (jsNode instanceof JSONArray) {
            JSONArray jsArr = (JSONArray) jsNode;
            if (node.idx >= 0) {
                final Object o = jsArr.get(node.idx);
                if (node.next == null) {
                    list.add(o);
                } else {
                    find(node.next, list, o, pathToMatch);
                }
            } else if (node.includeAll) {
                if (node.next == null) {
                    list.add(jsArr);
                } else {
                    find(node.next, list, jsArr, pathToMatch);
                }
            }
        } else {
            list.add(jsNode);
        }
    }


    public static class Node {
        Node next;
        Node prev;
        String name;
        int idx = -1;
        int type = FROM_ROOT;
        boolean includeAll;
        int predicateType = NONE;
        String predicateName;
        String predicateTest;

        public final static int FROM_ROOT = 1;
        public final static int FROM_ANYWHERE = 2;
        public final static int INNER_NODE = 3;

        public final static int NONE = 0;
        public final static int EQUALS = 100;


        public Node(String name, int type, int predicateType, String predicateName, String predicateTest) {
            this.name = name;
            this.type = type;
            this.predicateName = predicateName;
            this.predicateType = predicateType;
            this.predicateTest = predicateTest;
        }

        public Node(String name, int type) {
            this(name, type, NONE, null, null);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Node{");
            sb.append("name='").append(name).append('\'');
            sb.append(", type=").append(type);
            if (idx >= 0) {
                sb.append(", idx=").append(idx);
            }
            sb.append('}');
            return sb.toString();
        }

        public String getKey() {
            StringBuilder sb = new StringBuilder();
            sb.append(name);
            Node p = prev;
            while(p != null) {
                sb.append('.').append(p.name);
                p = p.prev;
            }
            return sb.toString();
        }

        public boolean hasPredicate() {
            return predicateType != NONE;
        }

        public boolean testPredicate(JSONObject json) {
            if (json.has(predicateName)) {
                if (predicateType == EQUALS) {
                    return json.getString(predicateName).equals(predicateTest);
                }
            }
            return false;
        }
    }

    public static class Path {
        Node start;
        Node current;

        public Path(Node start) {
            this.start = start;
            this.current = this.start;
        }

        public void add(Node node) {
            this.current.next = node;
            node.prev = this.current;
            this.current = node;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Path{");
            Node p = start;
            while (p != null) {
                sb.append("\n\t").append(p);
                p = p.next;
            }
            sb.append('}');
            return sb.toString();
        }

    }


    public static void testParser() throws Exception {

        String jsonPath = "$..author";
        jsonPath = "$.store.book[*].author";
        jsonPath = "$..book[2]";
        jsonPath = "$.store..price";
        jsonPath = "$['store']['book']['title']";
        jsonPath = "$.'$'[0]";
        jsonPath = "$..'$'[0]";
        jsonPath = "$.store..'$'[0]";

        JsonPathParser parser = new JsonPathParser(jsonPath);

        final Path path = parser.parse();
        System.out.println(path);

        JSONObject json = new JSONObject(TEST);
        JSONPathProcessor processor = new JSONPathProcessor();

        // final List<Object> list = processor.find("$.store.book[2].author", json);
        //final List<Object> list = processor.find("$.store..price", json);
        //final List<Object> list = processor.find("$..book[2]", json);
        final List<Object> list = processor.find("$.store.book[*].author", json);
        for (Object o : list) {
            System.out.println(o);
        }

    }


    static final String TEST = "{ \"store\": {\n" +
            "    \"book\": [ \n" +
            "      { \"category\": \"reference\",\n" +
            "        \"author\": \"Nigel Rees\",\n" +
            "        \"title\": \"Sayings of the Century\",\n" +
            "        \"price\": 8.95\n" +
            "      },\n" +
            "      { \"category\": \"fiction\",\n" +
            "        \"author\": \"Evelyn Waugh\",\n" +
            "        \"title\": \"Sword of Honour\",\n" +
            "        \"price\": 12.99\n" +
            "      },\n" +
            "      { \"category\": \"fiction\",\n" +
            "        \"author\": \"Herman Melville\",\n" +
            "        \"title\": \"Moby Dick\",\n" +
            "        \"isbn\": \"0-553-21311-3\",\n" +
            "        \"price\": 8.99\n" +
            "      },\n" +
            "      { \"category\": \"fiction\",\n" +
            "        \"author\": \"J. R. R. Tolkien\",\n" +
            "        \"title\": \"The Lord of the Rings\",\n" +
            "        \"isbn\": \"0-395-19395-8\",\n" +
            "        \"price\": 22.99\n" +
            "      }\n" +
            "    ],\n" +
            "    \"bicycle\": {\n" +
            "      \"color\": \"red\",\n" +
            "      \"price\": 19.95\n" +
            "    }\n" +
            "  }\n" +
            "}\n" +
            "\n" +
            "XPath \tJSONPath";

    public static void main(String[] args) throws Exception {

        testParser();
    }
}
