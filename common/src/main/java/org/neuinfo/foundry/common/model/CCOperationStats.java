package org.neuinfo.foundry.common.model;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by bozyurt on 7/20/15.
 */
public class CCOperationStats {
    private String consumerCoordinatorId;
    List<ConsumerOperationStats> cosList = new LinkedList<ConsumerOperationStats>();
    Date lastUpdated;

    public CCOperationStats(String consumerCoordinatorId) {
        this.consumerCoordinatorId = consumerCoordinatorId;
    }

    public void addConsumerOpStats(ConsumerOperationStats cos) {
        if (cosList.contains(cos)) {
            cosList.add(cos);
        }
    }

    public String getConsumerCoordinatorId() {
        return consumerCoordinatorId;
    }

    public List<ConsumerOperationStats> getCosList() {
        return cosList;
    }

    public Date getLastUpdated() {
        if (lastUpdated == null && cosList != null && !cosList.isEmpty()) {
            lastUpdated = cosList.get(0).getLastUpdated();
        }
        return lastUpdated;
    }

    public JSONObject toJSON() {
        JSONObject json = new JSONObject();
        json.put("ccId", consumerCoordinatorId);
        JSONArray jsArr = new JSONArray();
        json.put("cosList", jsArr);
        for (ConsumerOperationStats cos : cosList) {
            jsArr.put(cos.toJSON());
        }
        return json;
    }

    public static CCOperationStats fromJSON(JSONObject json) {
        String ccId = json.getString("ccId");
        CCOperationStats ccos = new CCOperationStats(ccId);
        JSONArray jsArr = json.getJSONArray("cosList");
        for (int i = 0; i < jsArr.length(); i++) {
            ccos.addConsumerOpStats(ConsumerOperationStats.fromJSON(jsArr.getJSONObject(i)));
        }
        return ccos;
    }
}
