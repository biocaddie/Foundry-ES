package org.neuinfo.foundry.common.model;

import org.json.JSONObject;
import org.neuinfo.foundry.common.util.JSONUtils;

import java.text.ParseException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by bozyurt on 7/20/15.
 */
public class ConsumerOperationStats {
    private String consumerId;
    private AtomicInteger numDocsProcessed = new AtomicInteger(0);
    private Date lastUpdated;
    private String consumerName;

    public ConsumerOperationStats(String consumerId, String consumerName) {
        this.consumerId = consumerId;
        this.consumerName = consumerName;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public int getNumDocsProcessed() {
        return numDocsProcessed.get();
    }


    public int incr() {
        return numDocsProcessed.incrementAndGet();
    }

    public Date getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public JSONObject toJSON() {
        JSONObject json = new JSONObject();
        json.put("consumerId", consumerId);
        json.put("name", consumerName);
        json.put("numDocsProcessed", numDocsProcessed.intValue());
        json.put("lastUpdated", JSONUtils.toBsonDate(lastUpdated));
        return json;
    }

    public static ConsumerOperationStats fromJSON(JSONObject json) {
        String consumerId = json.getString("consumerId");
        String name = json.getString("name");
        ConsumerOperationStats cos = new ConsumerOperationStats(consumerId, name);
        cos.numDocsProcessed.set( json.getInt("numDocsProcessed"));
        Date lastUpdated = null;
        try {
            lastUpdated = JSONUtils.fromBsonDate(json.getString("lastUpdated"));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        cos.setLastUpdated(lastUpdated);
        return cos;
    }
}
