package org.neuinfo.foundry.common.config;

import org.jdom2.Element;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 4/24/14.
 */
public class Route {
    Condition condition;
    List<QueueInfo> queueNames = new ArrayList<QueueInfo>(1);

    public Route(Condition condition) {
        this.condition = condition;
    }

    public Condition getCondition() {
        return condition;
    }

    public List<QueueInfo> getQueueNames() {
        return queueNames;
    }

    public static Route fromXml(Element elem, Map<String, QueueInfo> qiMap ) throws Exception{
        Element condElem = elem.getChild("condition");
        Element toElem = elem.getChild("to");

        Condition cond = Condition.fromXml(condElem);

        Route route = new Route(cond);
        String text = toElem.getTextTrim();
        String[] toks = text.split("\\s*,\\s*");
        for(String tok : toks) {
            QueueInfo queueInfo = qiMap.get(tok);
            if (queueInfo == null) {
                queueInfo = new QueueInfo(tok);
            }
            route.queueNames.add(queueInfo);
        }
        return route;
    }

    public static Route fromJSON(JSONObject json, Map<String, QueueInfo> qiMap) throws Exception {
        JSONObject condJson = json.getJSONObject("condition");
        JSONArray toJson = json.getJSONArray("to");
        Condition cond = Condition.fromJSON(condJson);
        Route route = new Route(cond);
        for(int i = 0; i < toJson.length(); i++) {
            String tok = toJson.getString(i);
            QueueInfo queueInfo = qiMap.get(tok);
            if (queueInfo == null) {
                queueInfo = new QueueInfo(tok);
            }
            route.queueNames.add(queueInfo);
        }
        return route;
    }

    public JSONObject toJSON() throws Exception {
        JSONObject json = new JSONObject();
        json.put("condition", condition.toJSON());
        JSONArray jsArr = new JSONArray();
        json.put("to", jsArr);
        for(QueueInfo queueInfo : queueNames) {
            jsArr.put(queueInfo.getName());
        }
        return json;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Route{");
        sb.append("condition=").append(condition);
        sb.append(", queueNames=").append(queueNames);
        sb.append('}');
        return sb.toString();
    }
}
