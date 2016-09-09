package org.neuinfo.foundry.common.model;

/**
 * Created by bozyurt on 3/1/16.
 */
public class ColumnMeta {
    final String name;
    final String type;

    public ColumnMeta(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }
}
