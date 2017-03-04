package org.neuinfo.foundry.consumers.plugin;

/**
 * Created by bozyurt on 10/28/14.
 */
public interface Ingestor extends IngestorIterable {
    public String getName();

    /**
     * @return -1 if not preloaded else if records come form the same document
     * the number of records in that document
     */
    public int getNumRecords();

    public String getOption(String optionName);

    public void shutdown();

}
