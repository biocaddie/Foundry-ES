package org.neuinfo.foundry.consumers;

import junit.framework.TestCase;
import org.neuinfo.foundry.consumers.common.CSVFileIterator;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.RemoteFileIterator;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bozyurt on 10/9/15.
 */
public class CSVFileIteratorTests extends TestCase {
    public CSVFileIteratorTests(String name) {
        super(name);
    }

    public void testGemmaRecIteration() throws Exception {
        File rootPath = new File("/var/data/foundry-es/test/gemma");
        List<File> csvFiles = new ArrayList<File>(1);
        csvFiles.add(new File(rootPath, "DatasetDiffEx.view_sample.txt"));
        CSVFileIterator it = new CSVFileIterator(new RemoteFileIterator(csvFiles), "\t");
        int count = 0;
        while (it.hasNext()) {
            List<String> row = it.next();
            System.out.println(row);
            ++count;
        }
        System.out.println("count:" + count);
        assertEquals(1000, count);
    }
}
