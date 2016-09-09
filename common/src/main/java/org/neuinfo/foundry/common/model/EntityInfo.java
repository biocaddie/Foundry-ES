package org.neuinfo.foundry.common.model;

import org.json.JSONObject;

/**
* Created by bozyurt on 2/11/15.
*/
public class EntityInfo {
    final String contentLocation;
    final String id;
    final int start;
    final int end;
    final String category;

    public EntityInfo(String contentLocation, String id, int start, int end, String category) {
        this.contentLocation = contentLocation;
        this.id = id;
        this.start = start;
        this.end = end;
        this.category = category;
    }

    public String getContentLocation() {
        return contentLocation;
    }

    public String getId() {
        return id;
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }

    public String getCategory() {
        return category;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("EntityInfo{");
        sb.append("contentLocation='").append(contentLocation).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append(", start=").append(start);
        sb.append(", end=").append(end);
        sb.append(", category=").append(category);
        sb.append('}');
        return sb.toString();
    }

    public JSONObject toJSON() {
        JSONObject js = new JSONObject();
        js.put("contentLocation", contentLocation);
        js.put("id", id);
        js.put("start", start);
        js.put("end", end);
        js.put("category", category);
        return js;
    }

    public static EntityInfo fromJSON(JSONObject json) {
        String contentLocation = json.getString("contentLocation");
        String id = json.getString("id");
        int start = json.getInt("start");
        int end = json.getInt("end");
        String category = json.getString("category");
        return new EntityInfo(contentLocation, id, start, end, category);
    }
}
