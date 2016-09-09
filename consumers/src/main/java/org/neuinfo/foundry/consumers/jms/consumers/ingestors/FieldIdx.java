package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

/**
 * Created by bozyurt on 10/21/15.
 */
class FieldIdx {
    String fieldName;
    int idx;

    public FieldIdx(String fieldName, int idx) {
        this.fieldName = fieldName;
        this.idx = idx;
    }
}
