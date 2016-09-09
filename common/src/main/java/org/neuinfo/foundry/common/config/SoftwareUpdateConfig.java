package org.neuinfo.foundry.common.config;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bozyurt on 9/25/14.
 */
public class SoftwareUpdateConfig {
    UpdateStatus updateStatus = UpdateStatus.NONE;
    List<SoftwareUpdateRec> updateRecords = new ArrayList<SoftwareUpdateRec>(10);



    public enum UpdateStatus {
        NONE, SCHEDULED, IN_PROC, FINISHED
    }

    public static class SoftwareUpdateRec {
        String version2Update;
        String handlerName;
        String repositoryURL;

        public SoftwareUpdateRec(String handlerName, String version2Update, String repositoryURL) {
            this.handlerName = handlerName;
            this.version2Update = version2Update;
            this.repositoryURL = repositoryURL;
        }

        public String getVersion2Update() {
            return version2Update;
        }

        public String getHandlerName() {
            return handlerName;
        }

        public String getRepositoryURL() {
            return repositoryURL;
        }
    }
}
