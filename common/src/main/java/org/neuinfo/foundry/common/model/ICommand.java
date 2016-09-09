package org.neuinfo.foundry.common.model;

/**
 * Created by bozyurt on 2/9/16.
 */
public interface ICommand {
    public ICommandOutput handle(ICommandInput input);
}
