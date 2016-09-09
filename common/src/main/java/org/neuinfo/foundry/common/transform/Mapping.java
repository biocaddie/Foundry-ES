package org.neuinfo.foundry.common.transform;

/**
 * Created by bozyurt on 4/15/15.
 */
public class Mapping {
    String refSourceID;
    String jsonPathForPK;
    String refJsonPath;
    String fromColumnName;
    /** path to model destination */
    String modelDestination;

    public String getFromColumnName() {
        return fromColumnName;
    }

    public void setFromColumnName(String fromColumnName) {
        this.fromColumnName = fromColumnName;
    }


    public String getModelDestination() {
        return modelDestination;
    }


    public void setModelDestination(String modelDestination) {
        this.modelDestination = modelDestination;
    }

    public String getRefSourceID() {
        return refSourceID;
    }

    public void setRefSourceID(String refSourceID) {
        this.refSourceID = refSourceID;
    }

    public String getJsonPathForPK() {
        return jsonPathForPK;
    }

    public void setJsonPathForPK(String jsonPathForPK) {
        this.jsonPathForPK = jsonPathForPK;
    }

    public String getRefJsonPath() {
        return refJsonPath;
    }

    public void setRefJsonPath(String refJsonPath) {
        this.refJsonPath = refJsonPath;
    }
}
