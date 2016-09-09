package org.neuinfo.foundry.common.model;

import java.io.File;
import java.util.List;

/**
 * Created by bozyurt on 2/9/16.
 */
public interface ICommandInput {
    public String getParam(String paramName);
    public void setParam(String paramName, String value);

    public List<File> getFiles();
    public void setFiles(List<File> files);
}
