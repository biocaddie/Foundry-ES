package org.neuinfo.foundry.consumers.common;

import org.json.JSONObject;
import org.neuinfo.foundry.common.model.ColumnMeta;

import java.util.List;
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
    public List<ColumnMeta> getColumnMetaList();

}
