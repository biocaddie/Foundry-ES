package org.neuinfo.foundry.consumers.common;

import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.GUnzip;

import java.io.File;

/**
 * Created by bozyurt on 4/30/15.
 */
public class Gunzipper extends GUnzip{
    public Gunzipper(File gzipFile) {
        super();
        setProject(new Project());
        setTaskName("gunzip");
        setSrc(gzipFile);
    }

    public void expandFile() {
        super.execute();
    }
}
