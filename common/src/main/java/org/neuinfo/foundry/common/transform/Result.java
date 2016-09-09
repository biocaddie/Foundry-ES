package org.neuinfo.foundry.common.transform;

import java.util.List;
/**
 * Created by bozyurt on 4/14/15.
 */
public class Result {
    String value;
    List<String> values;

    public Result(String value) {
        this.value = value;
    }

    public Result(List<String> values) {
        this.values = values;
    }

    public String getValue() {
        return value;
    }

    public List<String> getValues() {
        return values;
    }

    public boolean hasMultipleValues() {
        return values != null;
    }

}
