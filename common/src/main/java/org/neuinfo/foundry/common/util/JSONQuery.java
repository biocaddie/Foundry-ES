package org.neuinfo.foundry.common.util;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by bozyurt on 4/23/14.
 */
public class JSONQuery {

    public static List<JSONObject> findNodeByElemName(JSONObject json, String elemName) throws JSONException {
        if (json == null) {
            return new LinkedList<JSONObject>();
        }
        List<JSONObject> foundOnes = new LinkedList<JSONObject>();
        findNodeByElemName(elemName, json, foundOnes);

        return foundOnes;
    }

    private static void findNodeByElemName(String elemName, JSONObject parent,
                                           List<JSONObject> foundOnes) throws JSONException {
        if (parent.has("_name") && elemName.equals(parent.getString("_name"))) {
            foundOnes.add(parent);
        }

        if (parent.has("_children")) {
            JSONArray children = parent.getJSONArray("_children");
            for (int i = 0; i < children.length(); i++) {
                JSONObject child = children.getJSONObject(i);
                findNodeByElemName(elemName, child, foundOnes);
            }
        }
    }

}
