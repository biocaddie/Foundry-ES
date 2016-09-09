package org.neuinfo.foundry.common.util;

import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.Expand;

import java.io.File;

/**
 * Created by bozyurt on 12/17/15.
 */
public class Unzipper extends Expand {

    public Unzipper(File zipFile, File destFile) {
        super();
        setProject(new Project());
        getProject().init();
        setTaskName("unzip");
        setTaskType("unzip");
        setSrc(zipFile);
        setDest(destFile);
    }

    public void expand() {
        super.execute();
    }


}
