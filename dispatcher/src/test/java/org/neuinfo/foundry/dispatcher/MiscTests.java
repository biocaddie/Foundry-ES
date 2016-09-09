package org.neuinfo.foundry.dispatcher;

import junit.framework.TestCase;
import org.neuinfo.foundry.common.config.*;

/**
 * Created by bozyurt on 5/28/14.
 */
public class MiscTests extends TestCase {

    public MiscTests(String name) {
        super(name);
    }

    public void testConfigLoader() throws Exception {
        final Configuration conf = ConfigLoader.load("dispatcher-cfg.xml");
        System.out.println(conf.toString());

        assertEquals(1, conf.getWorkflows().size());
        Workflow wf = conf.getWorkflows().get(0);
        final Route route = wf.getRoutes().get(1);
        assertEquals(1, route.getQueueNames().size());
        final QueueInfo queueInfo = route.getQueueNames().get(0);
        assertTrue(queueInfo.hasHeaderFields());
        assertEquals(2, queueInfo.getHeaderFieldSet().size());

        System.out.println( wf.toJSON().toString(2) );
    }
}
