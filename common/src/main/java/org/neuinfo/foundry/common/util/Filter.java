package org.neuinfo.foundry.common.util;

import org.json.JSONObject;

import java.util.List;

/**
 * Created by bozyurt on 12/14/16.
 */
public class Filter {
    private String filterJsonPath;
    final String filterValue;

    public Filter(String filterJsonPath, String filterValue) {
        this.filterJsonPath = filterJsonPath;
        this.filterValue = filterValue;
        if (!this.filterJsonPath.startsWith("$.")) {
            this.filterJsonPath = "$." + this.filterJsonPath;
        }
    }


    public boolean satisfied(JSONObject json) {
        JSONPathProcessor2 processor = new JSONPathProcessor2();
        List<JSONPathProcessor2.JPNode> list;
        try {
            list = processor.find(filterJsonPath, json);
            if (list != null && !list.isEmpty()) {
                JSONPathProcessor2.JPNode jpNode = list.get(0);
                return jpNode.getValue().equals(filterValue);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
