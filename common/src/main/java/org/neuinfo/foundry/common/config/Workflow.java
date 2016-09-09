package org.neuinfo.foundry.common.config;

import org.jdom2.Element;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 9/24/14.
 */
public class Workflow {
    String name;
    List<Route> routes = new ArrayList<Route>(10);


    public Workflow(String name) {
        this.name = name;
    }


    public String getName() {
        return name;
    }

    public List<Route> getRoutes() {
        return routes;
    }

    public static Workflow fromXml(Element elem, Map<String, QueueInfo> qiMap) throws Exception {
        String name = elem.getAttributeValue("name");
        Workflow w = new Workflow(name);
        Element routesEl = elem.getChild("routes");
        List<Element> children = routesEl.getChildren("route");
        for (Element child : children) {
            w.routes.add(Route.fromXml(child, qiMap));
        }
        return w;
    }

    public static Workflow fromJSON(JSONObject json, Map<String, QueueInfo> qiMap) throws Exception {
        String name = json.getString("name");
        Workflow w = new Workflow(name);
        JSONArray jsArr = json.getJSONArray("routes");
        for(int i = 0; i < jsArr.length(); i++) {
            JSONObject js = jsArr.getJSONObject(i);
            w.routes.add( Route.fromJSON(js, qiMap));
        }
        return w;
    }

    public JSONObject toJSON() throws Exception {
        JSONObject json = new JSONObject();
        json.put("name", name);
        JSONArray jsArr = new JSONArray();
        json.put("routes", jsArr);
        for(Route r : routes) {
            jsArr.put( r.toJSON());
        }
        return json;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Workflow::[");
        sb.append("name='").append(name).append('\'');
        for(Route r : routes) {
            sb.append("\n\t").append(r.toString());
        }
        sb.append("\n]");
        return sb.toString();
    }
}
