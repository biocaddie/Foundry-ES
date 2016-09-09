package org.neuinfo.foundry.common.model;

import org.neuinfo.foundry.common.transform.IdentityTransformationGenerator;

import java.util.*;

/**
 * Created by bozyurt on 3/9/16.
 */
public class JsonNode {
    Object json;
    String name;
    int idx = -1;
    JsonNode parent = null;
    boolean array = false;
    List<JsonNode> children = new LinkedList<JsonNode>();

    public JsonNode(Object json, String name, JsonNode parent, boolean array, int idx) {
        this.json = json;
        this.name = name;
        this.parent = parent;
        this.array = array;
        this.idx = idx;
    }

    public JsonNode(Object json, String name, JsonNode parent, boolean array) {
        this(json, name, parent, array, -1);
    }

    public JsonNode addChild(Object json, String name) {
        return addChild(json, name, false, -1);
    }

    public JsonNode addChild(Object json, String name, boolean array, int idx) {
        JsonNode child = new JsonNode(json, name, this, array, idx);
        children.add(child);
        return child;
    }

    public boolean isArray() {
        return array;
    }

    public boolean hasChildren() {
        return !children.isEmpty();
    }

    public Object getJson() {
        return json;
    }

    public String getName() {
        return name;
    }

    public JsonNode getParent() {
        return parent;
    }

    public List<JsonNode> getChildren() {
        return children;
    }

    public void setArray(boolean array) {
        this.array = array;
    }

    public int getIdx() {
        return idx;
    }

    public String toDotNotation() {
        List<JsonNode> path = new ArrayList<JsonNode>(10);
        JsonNode n = this;
        while( n != null) {
            path.add(n);
            n = n.getParent();
            if (n != null && n.getParent() == null) {
                break;
            }
        }
        Collections.reverse(path);
        StringBuilder sb = new StringBuilder(128);
        int len = path.size();
        for(int i = 0; i < len; i++) {
            JsonNode node = path.get(i);
            if (node.isArray()) {
                sb.append(node.name).append('[').append(node.idx).append(']');

            } else {
                sb.append(node.name);
            }
            if ((i + 1) < len) {
                sb.append('.');
            }
        }
        return sb.toString();
    }
}
