package org.neuinfo.foundry.common.model;

import org.json.JSONObject;

/**
 * Created by bozyurt on 12/15/15.
 */
public interface IDataParser {
    public void initialize(InputDataIterator iterator) throws Exception;
    public JSONObject toJSON() throws Exception;
}
