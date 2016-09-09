package org.neuinfo.foundry.consumers;

import junit.framework.TestCase;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.JSONPathProcessor;
import org.neuinfo.foundry.common.util.XML2JSONConverter;
import org.neuinfo.foundry.consumers.jms.consumers.EntityAnnotationJMSConsumer;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by bozyurt on 6/5/14.
 */
public class EntityAnnotationConsumerTest extends TestCase {
    public EntityAnnotationConsumerTest(String testName) {
        super(testName);
    }


    public void testAnnotateEntities() throws Exception {
        String docUrl = "http://hydro10.sdsc.edu/metadata/ScienceBase_WAF_dump/00528801-446A-4CE2-BFDA-6F23E75820FA.xml";
        final JSONObject json = getAndConvertDoc(docUrl);

        JSONPathProcessor processor = new JSONPathProcessor();

        final List<Object> objects = processor.find("$..'gmd:abstract'.'gco:CharacterString'.'_$'", json);
        assertNotNull(objects);
        assertFalse(objects.isEmpty());
        String abstractText = (String) objects.get(0);

        Map<String,EntityAnnotationJMSConsumer.Keyword> keywordMap = new LinkedHashMap<String, EntityAnnotationJMSConsumer.Keyword>();

        EntityAnnotationJMSConsumer.annotateEntities("gmd:abstract", abstractText, keywordMap);

        assertTrue(!keywordMap.isEmpty());
        for(EntityAnnotationJMSConsumer.Keyword keyword : keywordMap.values()) {
            System.out.println(keyword);
        }

        System.out.println("==================================");
        JSONArray jsArr = new JSONArray();
        for(EntityAnnotationJMSConsumer.Keyword keyword : keywordMap.values()) {
            jsArr.put( keyword.toJSON() );
        }
        System.out.println( jsArr.toString(2));
    }

    public static JSONObject getAndConvertDoc(String xmlSource) throws Exception {
        SAXBuilder builder = new SAXBuilder();
        Document doc = builder.build(xmlSource);
        Element rootEl = doc.getRootElement();

        XML2JSONConverter converter = new XML2JSONConverter();
        final JSONObject json = converter.toJSON(rootEl);

        return json;

    }
}