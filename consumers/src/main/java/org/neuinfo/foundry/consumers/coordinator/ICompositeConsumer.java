package org.neuinfo.foundry.consumers.coordinator;

import org.neuinfo.foundry.consumers.plugin.IPlugin;

import java.util.List;

/**
 * Created by bozyurt on 12/8/15.
 */
public interface ICompositeConsumer extends IConsumer {

    public void addPlugin(IPlugin plugin);

    public List<IPlugin> getPlugins();
}
