package org.neuinfo.foundry.common.util;

import junit.framework.TestCase;
import org.jdom2.Element;
import org.json.JSONObject;

import java.io.File;
import java.util.List;

/**
 * Created by bozyurt on 5/27/14.
 */
public class JsonPathProcessorTest extends TestCase {

    public void testFindPrimaryKey() throws Exception {
        final String s = Utils.loadAsString("/tmp/osbp.json");
        JSONObject js = new JSONObject(s);

        JSONPathProcessor processor = new JSONPathProcessor();

        final List<Object> objects = processor.find("$.project.identifier.'_$'", js);

        assertTrue(objects.size() == 1);
        String identifier = objects.get(0).toString();

        assertEquals(identifier, "vogelsetal2011");
    }


    public void testPDBPath1() throws Exception {
        // File pdbFile = new File("/tmp/pdb_ftp/04/104d.xml");
        File pdbFile = new File("/var/temp/pdb_rsync/za/1za0-noatom.xml");
        Element rootEl = Utils.loadXML(pdbFile.getAbsolutePath());
        XML2JSONConverter converter = new XML2JSONConverter();
        JSONObject json = converter.toJSON(rootEl);
        JSONPathProcessor processor = new JSONPathProcessor();
        List<Object> objects = processor.find("$.'PDBx:datablock'.'PDBx:chem_compCategory'.'PDBx:chem_comp'[*].'PDBx:formula'.'_$'"
                , json);

        assertTrue(objects.size() == 6);
    }
}
