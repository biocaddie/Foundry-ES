package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.consumers.common.IngestFileCacheManager;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 10/21/15.
 */
public class RemoteFileIterator implements Iterator<File> {
    Iterator<File> localFilesIter;
    Map<String, List<String>> sourceData;
    String urlTemplate;
    List<String> templateVariables;
    File curFile = null;
    int curSourceRecIdx = -1;
    int numSourceRecs = -1;
    Map<String, FieldIdx> fieldMapping;
    IngestFileCacheManager cacheManager;

    public RemoteFileIterator(List<File> localFiles) {
        this.localFilesIter = localFiles.iterator();
    }

    public RemoteFileIterator(Map<String, List<String>> sourceData, String urlTemplate,
                              Map<String, FieldIdx> fieldMapping) {
        this.sourceData = sourceData;
        this.urlTemplate = urlTemplate;
        this.fieldMapping = fieldMapping;
        curSourceRecIdx = 0;
        numSourceRecs = sourceData.values().iterator().next().size();
        templateVariables = IngestorHelper.extractTemplateVariables(this.urlTemplate);
        Assertion.assertNotNull(templateVariables);
    }

    public void setCacheManager(IngestFileCacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    void retrieveNextFile() {
        Map<String, String> tv2ValueMap = new HashMap<String, String>();
        for (String templateVar : templateVariables) {
            List<String> list = sourceData.get(templateVar);
            tv2ValueMap.put(templateVar, list.get(curSourceRecIdx));
        }
        String ingestURL = IngestorHelper.createURL(this.urlTemplate, tv2ValueMap);
        try {
            if (cacheManager != null) {
                this.curFile = cacheManager.get(ingestURL);
                if (this.curFile == null) {
                    this.curFile = ContentLoader.getContent(ingestURL);
                    cacheManager.put(ingestURL, this.curFile);
                }
            } else {
                this.curFile = ContentLoader.getContent(ingestURL);
            }
            curSourceRecIdx++;
        } catch (Exception e) {
            e.printStackTrace();
            this.curFile = null;
        }
    }

    @Override
    public boolean hasNext() {
        if (sourceData != null) {
            return curSourceRecIdx < numSourceRecs;
        } else {
            return localFilesIter.hasNext();
        }
    }

    @Override
    public File next() {
        if (sourceData == null) {
            return localFilesIter.next();
        }
        retrieveNextFile();
        return curFile;
    }

    @Override
    public void remove() {
       throw new UnsupportedOperationException();
    }


    public String[] getAddedFieldsForCurrentSourceRec() {
        String[] addedFields4Record = null;
        addedFields4Record = new String[fieldMapping.size()];
        for (String origFieldName : fieldMapping.keySet()) {
            List<String> list = sourceData.get(origFieldName);
            FieldIdx fi = fieldMapping.get(origFieldName);
            addedFields4Record[fi.idx] = list.get(this.curSourceRecIdx - 1);
        }
        return addedFields4Record;
    }

}
