package org.neuinfo.foundry.consumers.common;

import org.json.JSONObject;
import org.neuinfo.foundry.common.model.ColumnMeta;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by bozyurt on 5/10/16.
 */
public class WebJoinIterator implements Iterator<JSONObject> {


    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public JSONObject next() {
        return null;
    }

    @Override
    public void remove() {

    }


    public static class WebTableMeta {
        final String url;
        final String alias;
        List<ColumnMeta> cmList;
        List<JoinInfo> jiList = new ArrayList<JoinInfo>(3);

        public WebTableMeta(String url, String alias) {
            this.url = url;
            this.alias = alias;
        }

         void addJoinInfo(JoinInfo ji) {
            jiList.add(ji);
        }
    }

}
