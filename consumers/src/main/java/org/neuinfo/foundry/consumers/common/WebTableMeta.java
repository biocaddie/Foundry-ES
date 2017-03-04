package org.neuinfo.foundry.consumers.common;

import org.neuinfo.foundry.common.model.ColumnMeta;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.IngestorHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bozyurt on 2/17/17.
 */
public class WebTableMeta {
    final String url;
    final String alias;
    String recordJsonPath;
    String fieldJsonPath;
    List<ColumnMeta> cmList;
    WebJoinIterator.WebJoinInfo joinInfo;

    public WebTableMeta(String url, String alias) {
        this.url = url;
        this.alias = alias;
    }


    public boolean hasParametrizedURL() {
        List<String> list = IngestorHelper.extractTemplateVariables(url);
        return list != null;
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


    public String getUrl() {
        return url;
    }

    public String getAlias() {
        return alias;
    }


    public void setJoinInfo(WebJoinIterator.WebJoinInfo joinInfo) {
        Assertion.assertTrue(this.joinInfo == null);
        this.joinInfo = joinInfo;
    }

    public WebJoinIterator.WebJoinInfo getJoinInfo() {
        return this.joinInfo;
    }

    public List<ColumnMeta> getCmList() {
        return cmList;
    }

    public void setCmList(List<ColumnMeta> cmList) {
        this.cmList = cmList;
    }
}
