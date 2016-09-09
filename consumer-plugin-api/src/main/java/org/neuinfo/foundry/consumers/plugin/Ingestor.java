package org.neuinfo.foundry.consumers.plugin;

import org.json.JSONObject;

import java.util.Map;

/**
 * Created by bozyurt on 10/28/14.
 */
public interface Ingestor {

    public void initialize(Map<String, String> options) throws Exception;

    public void startup() throws Exception;

    public Result prepPayload();

    public String getName();

    /**
     * @return -1 if not preloaded else if records come form the same document
     * the number of records in that document
     */
    public int getNumRecords();

    public String getOption(String optionName);

    public void shutdown();

    public boolean hasNext();
}
