package org.neuinfo.foundry.common.model;

import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.json.JSONObject;

/**
 * Created by bozyurt on 10/16/14.
 */
public class Organization {
    private String objectId;
    private String name;

    public Organization(String name) {
        this.name = name;
    }

    public String getObjectId() {
        return objectId;
    }

    public String getName() {
        return name;
    }

    public Organization(String objectId, String name) {
        this.objectId = objectId;
        this.name = name;
    }

    public JSONObject toJSON() {
        JSONObject js = new JSONObject();
        js.put("name", name);
        if (objectId != null) {
            js.put("id", objectId);
        }
        return js;
    }

    public static Organization fromDBObject(DBObject dbo) {
        String name = (String) dbo.get("name");
        String objectId = ((ObjectId) dbo.get("_id")).toHexString();

        return new Organization(objectId, name);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Organization{");
        sb.append("objectId='").append(objectId).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
