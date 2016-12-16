package org.neuinfo.foundry.consumers.ingestors;

import org.junit.Test;
import org.neuinfo.foundry.consumers.common.JsonRecordIterator;

import java.io.File;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by bozyurt on 12/9/16.
 */
public class JsonRecordIteratorTest {

    @Test
    public void testIterate() throws Exception {
        URL resource = JsonRecordIterator.class.getClassLoader().getResource("testdata/test.json");
       String path =  resource.toURI().getPath();
        JsonRecordIterator it = new JsonRecordIterator(new File(path), "data");
        assertTrue(it.hasNext());

        int count = 0;
        while (it.hasNext()) {
            System.out.println(it.next());
            count++;
        }
        assertEquals(4, count);
    }
}
