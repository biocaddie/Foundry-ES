package org.neuinfo.foundry.common.command;

import org.neuinfo.foundry.common.model.ICommand;

/**
 * Created by bozyurt on 2/9/16.
 */
public class CommandFactory {

    public static ICommand create(String commandName) {
        if (commandName.equals("combine")) {
            return new Combiner();
        } else {
            throw new RuntimeException("Unsupported command:" + commandName);
        }
    }
}
