package org.neuinfo.foundry.common.util;

import org.json.JSONArray;
import org.json.JSONObject;

import org.neuinfo.foundry.common.util.JSONPathProcessor.Node;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by bozyurt on 6/19/15.
 */
public class JSONPathTracer {
    PathNode root = null;


    public List<PathNode> find(String jsonPathExpr, JSONObject json) throws Exception {
        JsonPathParser parser = new JsonPathParser(jsonPathExpr);
        final JSONPathProcessor.Path path = parser.parse();
        root = new PathNode(null, null, null);
        List<PathNode> list = new ArrayList<PathNode>(10);
        find(path.start, list, json, root);
        return list;
    }

    private void find(Node node, List<PathNode> list, Object parent, PathNode parentPN) {
        if (node.type == Node.FROM_ROOT || node.type == Node.INNER_NODE) {

            if (parent instanceof JSONObject) {
                JSONObject p = (JSONObject) parent;
                if (!p.has(node.name)) {
                    return;
                }
                if (node.includeAll || node.idx >= 0) {
                    if (node.idx >= 0) {
                        final Object o = p.getJSONArray(node.name).get(node.idx);
                        PathNode pn = parentPN.addChild(o, node.name);
                        if (node.next == null) {
                            list.add(pn);
                        } else {
                            find(node.next, list, o, pn);
                        }
                    } else if (node.includeAll) {
                        final JSONArray jsArr = p.getJSONArray(node.name);
                        PathNode pn = parentPN.addChild(jsArr, node.name);
                        if (node.next == null) {
                            list.add(pn);
                        } else {
                            for (int i = 0; i < jsArr.length(); i++) {
                                find(node.next, list, jsArr.get(i), pn);
                            }
                        }
                    }
                } else {
                    Object js = p.get(node.name);
                    PathNode pn = parentPN.addChild(js, node.name);
                    if (node.next == null) {
                        list.add(pn);
                    } else {
                        find(node.next, list, js, pn);
                    }
                }
            }
        } else if (node.type == Node.FROM_ANYWHERE) {
            List<PathNode> pathNodes = new LinkedList<PathNode>();
            findMatching(node, parent, pathNodes, parentPN);

            if (!pathNodes.isEmpty()) {
                if (node.next == null) {
                    for (PathNode jsNode : pathNodes) {
                        list.add(jsNode);
                    }
                }
                for (PathNode jsNode : pathNodes) {
                    x(node.next, jsNode.json, list, jsNode);
                }
            }
        }
    }

    private void x(Node node, Object jsNode, List<PathNode> list, PathNode parentPN) {
        if (jsNode instanceof JSONObject) {
            JSONObject p = (JSONObject) jsNode;
            if (node.includeAll || node.idx >= 0) {
                if (node.idx >= 0) {
                    final Object o = p.getJSONArray(node.name).get(node.idx);
                    if (o instanceof JSONObject) {
                        JSONObject js = (JSONObject) o;
                        if (node.next == null) {
                            PathNode pn = parentPN.addChild(js, node.name);
                            list.add(pn);
                        } else {
                            find(node.next, list, js, parentPN);
                        }
                    } else if (jsNode instanceof JSONArray) {
                        //TODO
                    }
                }
            } else {
                if (p.has(node.name)) {
                    Object js = p.get(node.name);
                    PathNode pn = parentPN.addChild(js, node.name);
                    if (node.next == null) {
                        list.add(pn);
                    } else {
                        find(node.next, list, js, pn);
                    }
                }
            }
        } else if (jsNode instanceof JSONArray) {
            JSONArray jsArr = (JSONArray) jsNode;
            if (node.idx >= 0) {
                final Object o = jsArr.get(node.idx);
                PathNode pn = parentPN.addChild(o, node.name);
                if (node.next == null) {
                    list.add(pn);
                } else {
                    find(node.next, list, o, pn);
                }
            } else if (node.includeAll) {
                PathNode pn = parentPN.addChild(jsArr, node.name);
                if (node.next == null) {
                    list.add(pn);
                } else {
                    find(node.next, list, jsArr, parentPN);
                }
            }
        } else {
            PathNode pn = parentPN.addChild(jsNode, node.name);
            list.add(pn);
        }
    }

    private void findMatching(Node node, Object parent, List<PathNode> list, PathNode parentPN) {
        if (parent instanceof JSONObject) {
            JSONObject p = (JSONObject) parent;
            if (p.has(node.name)) {
                if (!node.hasPredicate()) {
                    PathNode pn = parentPN.addChild(p.get(node.name), node.name);
                    list.add(pn);
                } else {
                    Object obj = p.get(node.name);
                    if (obj instanceof JSONObject) {
                        if (node.testPredicate((JSONObject) obj)) {
                            PathNode pn = parentPN.addChild(obj, node.name);
                            list.add(pn);
                        }
                    } else if (obj instanceof JSONArray) {
                        JSONArray jsArr = (JSONArray) obj;
                        int len = jsArr.length();
                        for (int i = 0; i < len; i++) {
                            Object o = jsArr.get(i);
                            if (o instanceof JSONObject) {
                                if (node.testPredicate((JSONObject) o)) {
                                    PathNode pn = parentPN.addChild(o, node.name);
                                    list.add(pn);
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
                    PathNode pn = parentPN.addChild(child, key);
                    findMatching(node, child, list, pn);
                }
            }
        } else if (parent instanceof JSONArray) {
            findMatchingChildren(node, (JSONArray) parent, list, parentPN);
        }

    }

    private void findMatchingChildren(Node node, JSONArray parent, List<PathNode> list, PathNode parentPN) {
        JSONArray jsArr = parent;
        for (int i = 0; i < jsArr.length(); i++) {
            Object child = jsArr.get(i);
            if (child instanceof JSONObject || child instanceof JSONArray) {
                findMatching(node, child, list, parentPN);
            }
        }
    }

    public static class PathNode {
        Object json;
        String name;
        PathNode parent = null;
        List<PathNode> children = new LinkedList<PathNode>();

        public PathNode(Object json, String name, PathNode parent) {
            this.json = json;
            this.name = name;
            this.parent = parent;
        }

        public PathNode addChild(Object json, String name) {
            PathNode child = new PathNode(json, name, this);
            children.add(child);
            return child;
        }
    }

    public static void main(String[] args) throws Exception {
        String jsonStr = Utils.loadAsString("/tmp/pdb_xml_record.json");
        JSONObject docJSON = new JSONObject(jsonStr);
        JSONPathTracer tracer = new JSONPathTracer();
        List<PathNode> pathNodes = tracer.find("$..'PDBx:citation_author'[?(@.'@citation_id'='primary')].'@name'", docJSON);
        System.out.println(pathNodes);
    }
}
