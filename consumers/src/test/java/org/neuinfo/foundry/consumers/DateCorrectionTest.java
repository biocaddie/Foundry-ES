package org.neuinfo.foundry.consumers;

import junit.framework.TestCase;

/**
 * Created by bozyurt on 9/21/15.
 */
public class DateCorrectionTest extends TestCase {

    public DateCorrectionTest(String name) {
        super(name);
    }

    public void testCorrectDates() throws Exception {
        Helper helper = new Helper("");
        try {
            helper.startup("consumers-cfg.xml");

            helper.updateDates("nif-0000-00135","nifRecords");

        } finally {
            helper.shutdown();
        }
    }
}
