package org.neuinfo.foundry.jms.producer;

import org.neuinfo.foundry.common.util.Utils;

import java.util.Map;

/**
 * Created by bozyurt on 1/18/17.
 */
public interface ICommand {
    public void execute(Utils.OptParser optParser) throws Exception;
}
