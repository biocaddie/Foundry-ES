package org.neuinfo.foundry.common.model;

import java.io.File;
import java.util.List;

/**
 * Created by bozyurt on 2/9/16.
 */
public class CommandOutput implements ICommandOutput {
    List<File> files;
    public CommandOutput(List<File> files) {
        this.files = files;
    }

    @Override
    public List<File> getFiles() {
        return files;
    }
}
