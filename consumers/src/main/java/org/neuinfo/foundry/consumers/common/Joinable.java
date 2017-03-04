package org.neuinfo.foundry.consumers.common;

import org.json.JSONObject;

import java.util.Map;

/**
 * Created by bozyurt on 2/28/17.
 */
public interface Joinable {
    public void reset(String refValue) throws Exception;
    public JSONObject next();
    public boolean hasNext();
    public JSONObject peek();
    public String getJoinValue();
    public String getAlias();
    public void setResetJsonPath(String resetJsonPath);
    public void setJoinValueJsonPath(String joinValueJsonPath) throws Exception;

}
