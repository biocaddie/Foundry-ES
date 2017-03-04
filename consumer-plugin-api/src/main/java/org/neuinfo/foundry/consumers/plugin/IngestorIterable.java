package org.neuinfo.foundry.consumers.plugin;

import java.util.Map;

/**
 * Created by bozyurt on 2/27/17.
 */
public interface IngestorIterable {
    public void initialize(Map<String, String> options) throws Exception;

    public void startup() throws Exception;

    public Result prepPayload();
    public boolean hasNext();

}
