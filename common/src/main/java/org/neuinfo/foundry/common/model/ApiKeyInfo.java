package org.neuinfo.foundry.common.model;

import com.mongodb.DBObject;
import org.json.JSONObject;

/**
 * Created by bozyurt on 1/15/16.
 */
public class ApiKeyInfo {
    private String username;
    private String apiKey;
    private boolean perpetual;

    public ApiKeyInfo() {
    }

    public String getUsername() {
        return username;
    }

    public String getApiKey() {
        return apiKey;
    }

    public boolean isPerpetual() {
        return perpetual;
    }

    public static ApiKeyInfo fromJSON(JSONObject json) {
        ApiKeyInfo aki = new ApiKeyInfo();
        aki.apiKey = json.getString("apiKey");
        aki.username = json.getString("username");
        aki.perpetual = json.getBoolean("perpetual");
        return aki;
    }

    public static ApiKeyInfo fromDBObject(DBObject akiDBO) {
        ApiKeyInfo aki = new ApiKeyInfo();
        aki.username = (String) akiDBO.get("username");
        aki.apiKey = (String) akiDBO.get("apiKey");
        aki.perpetual = (Boolean) akiDBO.get("perpetual");
        return aki;
    }
}
