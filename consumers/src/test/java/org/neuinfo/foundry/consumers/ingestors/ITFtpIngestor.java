package org.neuinfo.foundry.consumers.ingestors;

import org.apache.commons.net.ftp.FTPFile;
import org.junit.Test;
import org.neuinfo.foundry.consumers.common.FtpClient;
import org.neuinfo.foundry.consumers.jms.consumers.ingestors.FTPIngestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * Created by bozyurt on 3/26/15.
 */
public class ITFtpIngestor {

    public void testFtpPDBRecursive() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ftpHost", "ftp.wwpdb.org");
        options.put("remotePath", "/pub/pdb/data/structures/divided/XML/");
        options.put("filenamePattern", ".+\\.xml\\.gz$");
        options.put("recursive", "true");
        options.put("outDir", "/tmp/pdb_ftp");
        options.put("documentElement", "datablock");
        FTPIngestor ingestor = new FTPIngestor();
        ingestor.setTestMode(true);
        ingestor.initialize(options);

        ingestor.startup();
        if (ingestor.hasNext()) {
            Result result = ingestor.prepPayload();
            assertNotNull(result.getPayload());

        }
        ingestor.shutdown();
    }

    @Test
    public void testFtp() throws Exception {
        Map<String, String> options = new HashMap<String, String>(17);
        options.put("ftpHost", "ftp.ebi.ac.uk");
        options.put("remotePath",
                "/pub/databases/biomodels/weekly_archives/2015/BioModels-Database-weekly-2015-03-23-sbmls.tar.bz2");
        options.put("outDir", "/var/data/foundry-es/test");
        options.put("filenamePattern", "MODEL\\d+\\.xml");
        options.put("documentElement", "sbml");
        options.put("pathPattern",
                "/pub/databases/biomodels/weekly_archives/%[\\d{4,4}]%/BioModels-Database-weekly-%[\\d+\\-\\d\\d\\-\\d\\d]%-sbmls.tar.bz2");
        options.put("pattern1Type", "date_yyyy");
        options.put("pattern2Type", "date_yyyy-MM-dd");
        FTPIngestor ingestor = new FTPIngestor();
        ingestor.initialize(options);

        // ingestor.startup();

        String latestFilePath = ingestor.determineMostRecentFile2Download();

        assertNotNull(latestFilePath);
        System.out.println("latestFilePath:" + latestFilePath);
    }

    @Test
    public void testFtpListing() throws Exception {
        FtpClient ftp = new FtpClient("ftp.ebi.ac.uk");
        List<FTPFile> list = ftp.list("/pub/databases/biomodels/weekly_archives");

        assertFalse(list.isEmpty());
        for (FTPFile f : list) {
            System.out.println(f.getName());
        }
    }
}
