package org.neuinfo.foundry.common.model;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 2/9/16.
 */
public class CommandInput implements ICommandInput {
    Map<String,String> map = new HashMap<String, String>(23);
    List<File> files;

    public CommandInput(List<File> files) {
        this.files = files;
    }

    @Override
    public String getParam(String paramName) {
        return map.get(paramName);
    }

    @Override
    public void setParam(String paramName, String value) {
         map.put(paramName, value);
    }

    @Override
    public List<File> getFiles() {
        return this.files;
    }

    @Override
    public void setFiles(List<File> files) {
        this.files = files;
    }
}
