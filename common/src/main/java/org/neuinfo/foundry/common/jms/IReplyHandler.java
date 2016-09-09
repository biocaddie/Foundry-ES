package org.neuinfo.foundry.common.jms;

import org.json.JSONObject;

/**
 * Created by bozyurt on 1/25/16.
 */
public interface IReplyHandler {

    public JSONObject handle(JSONObject requestJSON);
}
