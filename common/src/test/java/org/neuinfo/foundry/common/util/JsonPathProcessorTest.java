package org.neuinfo.foundry.common.util;

import org.json.JSONObject;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by bozyurt on 5/27/14.
 */
public class JsonPathProcessorTest {
    static String HOME = System.getProperty("user.home");

    @Test
    public void testFindPrimaryKey() throws Exception {
        String s = Utils.loadAsString(HOME + "/dev/biocaddie/data-pipeline/SampleData/pdb/5amh-noatom.json");
        JSONObject js = new JSONObject(s);

        JSONPathProcessor processor = new JSONPathProcessor();

        final List<Object> objects = processor.find("$.'PDBx:datablock'.'@datablockName'", js);

        assertEquals(objects.size(), 1);
        String identifier = objects.get(0).toString();
        System.out.println(identifier);

        assertEquals(identifier, "5AMH-noatom");
    }


    @Test
    public void testPDBPath1() throws Exception {
        /*
        File pdbFile = new File("/var/temp/pdb_rsync/za/1za0-noatom.xml");
        Element rootEl = Utils.loadXML(pdbFile.getAbsolutePath());
        XML2JSONConverter converter = new XML2JSONConverter();
        JSONObject json = converter.toJSON(rootEl);
        */
        JSONPathProcessor processor = new JSONPathProcessor();
        String s = Utils.loadAsString(HOME + "/dev/biocaddie/data-pipeline/SampleData/pdb/5amh-noatom.json");
        JSONObject json = new JSONObject(s);
        List<Object> objects = processor.find("$.'PDBx:datablock'.'PDBx:chem_compCategory'.'PDBx:chem_comp'[*].'PDBx:formula'.'_$'"
                , json);

        System.out.println(objects);
        System.out.println(objects.size());
        assertEquals(objects.size(), 26);
    }
}
