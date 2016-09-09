package org.neuinfo.foundry.common.model;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Created by bozyurt on 2/11/15.
 */
public class Keyword {
    private String term;
    List<EntityInfo> entityInfos = new LinkedList<EntityInfo>();

    public Keyword(String term) {
        this.term = term;
    }

    public void addEntityInfo(EntityInfo ei) {
        entityInfos.add(ei);
    }

    public String getTerm() {
        return term;
    }

    public List<EntityInfo> getEntityInfos() {
        return entityInfos;
    }


    public Set<String> getCategories() {
        Set<String> categories = new HashSet<String>(7);
        for (EntityInfo ei : entityInfos) {
            if (ei.getCategory().length() > 0) {
                categories.add(ei.getCategory());
            }
        }
        return categories;
    }

    public boolean hasCategory() {
        for (EntityInfo ei : entityInfos) {
            if (ei.getCategory().length() > 0) {
                return true;
            }
        }
        return false;
    }

    public boolean hasAnyCategory(Set<String> allowedSet) {
        for (EntityInfo ei : entityInfos) {
            if (ei.getCategory().length() > 0) {
                if (allowedSet.contains(ei.getCategory())) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean hasCategory(String category) {
        for (EntityInfo ei : entityInfos) {
            if (ei.getCategory().length() > 0) {
                if (ei.getCategory().equals(category)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Keyword{");
        sb.append("term='").append(term).append('\'');
        sb.append(", entityInfos=").append(entityInfos);
        sb.append('}');
        return sb.toString();
    }

    public JSONObject toJSON() {
        JSONObject js = new JSONObject();
        js.put("term", term);
        JSONArray jsArr = new JSONArray();
        js.put("entityInfos", jsArr);
        for (EntityInfo ei : entityInfos) {
            jsArr.put(ei.toJSON());
        }
        return js;
    }

    public static Keyword fromJSON(JSONObject json) {
        String term = json.getString("term");
        Keyword kw = new Keyword(term);
        JSONArray jsArr = json.getJSONArray("entityInfos");

        for (int i = 0; i < jsArr.length(); i++) {
            JSONObject js = jsArr.getJSONObject(i);
            kw.addEntityInfo(EntityInfo.fromJSON(js));
        }
        return kw;
    }
}
