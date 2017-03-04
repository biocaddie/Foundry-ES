package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import org.json.JSONObject;
import org.neuinfo.foundry.consumers.common.WebJoinIterator;
import org.neuinfo.foundry.consumers.plugin.Ingestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.util.Map;

/**
 * Created by bozyurt on 2/21/17.
 */
public class WebJoinIngestor implements Ingestor {
    Map<String, String> optionMap;
    WebJoinIterator joinIterator;

    @Override
    public void initialize(Map<String, String> options) throws Exception {
        this.optionMap = options;

    }

    @Override
    public void startup() throws Exception {
        this.joinIterator = new WebJoinIterator(this.optionMap);
        this.joinIterator.startup();
    }

    @Override
    public Result prepPayload() {
        try {
            JSONObject json = joinIterator.next();
            return new Result(json, Result.Status.OK_WITH_CHANGE);
        } catch (Throwable t) {
            t.printStackTrace();
            return new Result(new JSONObject(), Result.Status.ERROR);
        }
    }

    @Override
    public String getName() {
        return "WebJoinIngestor";
    }

    @Override
    public int getNumRecords() {
        return -1;
    }

    @Override
    public String getOption(String optionName) {
        return optionMap.get(optionName);
    }

    @Override
    public void shutdown() {

    }

    @Override
    public boolean hasNext() {
        return this.joinIterator.hasNext();
    }
}
