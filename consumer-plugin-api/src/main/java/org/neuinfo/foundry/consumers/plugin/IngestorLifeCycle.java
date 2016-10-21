package org.neuinfo.foundry.consumers.plugin;

/**
 * Created by bozyurt on 10/12/16.
 */
public interface IngestorLifeCycle {

    /**
     * any postprocessing after ingestion is done here
     */
    public void beforeShutdown(IDocUpdater docUpdater);

}
