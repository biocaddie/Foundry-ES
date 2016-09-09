package org.neuinfo.foundry.consumers.plugin;

/**
 * Created by bozyurt on 10/28/14.
 */
public interface Ingestable {
    public void setIngestor(Ingestor ingestor);

    public Ingestor getIngestor();
}
