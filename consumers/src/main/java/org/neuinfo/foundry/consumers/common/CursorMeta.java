package org.neuinfo.foundry.consumers.common;


/**
 * Created by bozyurt on 2/28/17.
 */
public class CursorMeta {
    final String alias;
    final Joinable cursor;
    String recordJsonPath;
    String fieldJsonPath;
    WebJoinIterator.WebJoinInfo joinInfo;

    public CursorMeta(String alias, Joinable cursor) {
        this.alias = alias;
        this.cursor = cursor;
    }

    public String getRecordJsonPath() {
        if (recordJsonPath == null) {
            if (joinInfo != null) {
                this.recordJsonPath = joinInfo.primaryRecordJsonPath;
            }
        }
        return recordJsonPath;
    }

    public String getFieldJsonPath() {
        if (fieldJsonPath == null) {
            if (joinInfo != null) {
                this.fieldJsonPath = joinInfo.primaryJsonPath;
            }
        }
        return fieldJsonPath;
    }

    public boolean isLastInChain() {
        if (joinInfo == null) {
            return true;
        }
        return joinInfo.getSecondaryJsonPath() == null;
    }

    public String getAlias() {
        return alias;
    }

    public Joinable getCursor() {
        return cursor;
    }

    public WebJoinIterator.WebJoinInfo getJoinInfo() {
        return joinInfo;
    }

    public void setJoinInfo(WebJoinIterator.WebJoinInfo joinInfo) {
        this.joinInfo = joinInfo;
    }

    public void setRecordJsonPath(String recordJsonPath) {
        this.recordJsonPath = recordJsonPath;
    }

    public void setFieldJsonPath(String fieldJsonPath) {
        this.fieldJsonPath = fieldJsonPath;
    }
}
