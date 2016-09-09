package org.neuinfo.foundry.consumers;

import junit.framework.TestCase;
import org.bson.types.ObjectId;
import org.jdom2.Element;
import org.json.JSONObject;
import org.neuinfo.foundry.common.ingestion.GridFSService;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.common.util.XML2JSONConverter;
import org.neuinfo.foundry.consumers.common.ConfigLoader;
import org.neuinfo.foundry.consumers.common.Configuration;

/**
 * Created by bozyurt on 5/14/15.
 */
public class GridFSServiceTest extends TestCase {

    public GridFSServiceTest(String name) {
        super(name);
    }

    public void testDeleteJsonFile() throws Exception {
        ObjectId id = new ObjectId("5554f43fe4b043a25d87b6de");
        GridFSService service = new GridFSService();
        try {
            Configuration config = ConfigLoader.load("consumers-cfg.xml");
            service.start(config);

            service.deleteJSONFile(id);

        } finally {
            service.shutdown();
        }
    }

    public void testFindJsonFile() throws Exception {
        ObjectId id = new ObjectId("5554f43fe4b043a25d87b6de");
        GridFSService service = new GridFSService();
        try {
            Configuration config = ConfigLoader.load("consumers-cfg.xml");
            service.start(config);

            JSONObject json = service.findJSONFile(id);
            assertNotNull(json);
            System.out.println(json.toString(2));
        } finally {
            service.shutdown();
        }
    }

    public void testSaveJsonFile() throws Exception {
        String filename = "/tmp/pdb_ftp/00/100d.xml";
        Element rootEl = Utils.loadXML(filename);
        XML2JSONConverter converter = new XML2JSONConverter();
        JSONObject json = converter.toJSON(rootEl);
        GridFSService service = new GridFSService();
        try {
            Configuration config = ConfigLoader.load("consumers-cfg.xml");
            service.start(config);

            ObjectId objectId = service.saveJsonFile(json, filename);
            assertNotNull(objectId);

        } finally {
            service.shutdown();
        }
    }
}
