package org.neuinfo.foundry.consumers;

import junit.framework.TestCase;
import org.neuinfo.foundry.common.ingestion.DocProcessingStatsService;
import org.neuinfo.foundry.common.ingestion.DocProcessingStatsService.SourceStats;

import java.util.List;

/**
 * Created by bozyurt on 12/9/15.
 */
public class DocProcessingStatsServiceTests extends TestCase {
    public DocProcessingStatsServiceTests(String name) {
        super(name);
    }

    public void testCountAggregation() throws Exception {
        Helper helper = new Helper("");
        try {
            helper.startup("consumers-cfg.xml");
            DocProcessingStatsService dpss = new DocProcessingStatsService();
            try {
                dpss.start(helper.getConfig());
                List<SourceStats> ssList = dpss.getDocCountsPerStatusPerSource("nifRecords");
                for (SourceStats ss : ssList) {
                    System.out.println(ss);
                }
            } finally {
                dpss.shutdown();
            }

        } finally {
            helper.shutdown();
        }
    }

    public void testDocCounts2() throws Exception {
        Helper helper = new Helper("");
        try {
            helper.startup("consumers-cfg.xml");
            DocProcessingStatsService dpss = new DocProcessingStatsService();
            try {
                dpss.start(helper.getConfig());
                List<SourceStats> ssList = dpss.getDocCountsPerStatusPerSource2("nifRecords");
                for (SourceStats ss : ssList) {
                    System.out.println(ss);
                }
            } finally {
                dpss.shutdown();
            }
        } finally {
            helper.shutdown();
        }
    }
}
