package org.neuinfo.foundry.common.provenance;


import junit.framework.TestCase;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.openprovenance.prov.json.Converter;
import org.openprovenance.prov.model.*;
import org.openprovenance.prov.xml.ProvFactory;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.text.SimpleDateFormat;
import java.util.*;

public class ProvJSONTests extends TestCase {


    public ProvJSONTests(String name) {
        super(name);
    }


    public void testIt() throws Exception {
        final DatatypeFactory factory = DatatypeFactory.newInstance();
        org.openprovenance.prov.model.ProvFactory pFactory = new ProvFactory();
        final Document doc = pFactory.newDocument();

        QualifiedName qn = pFactory.newQualifiedName("http://example.org", "doc1", "foundry");

        final Entity entity1 = pFactory.newEntity(qn, "document");
        doc.getStatementOrBundle().add(entity1);


        QualifiedName id2 = pFactory.newQualifiedName("http://example.org", "docIdAssigner", "foundry");
        Attribute softwareAgent = pFactory.newAttribute("http://www.w3.org/ns/prov#", "type", "prov",
                "prov:SoftwareAgent",
                pFactory.newQualifiedName("http://www.w3.org/2001/XMLSchema", "string", "xsd"));
        final Agent consumer = pFactory.newAgent(id2, Arrays.asList(softwareAgent));


        doc.getStatementOrBundle().add(consumer);

        QualifiedName id3 = pFactory.newQualifiedName("http://example.org", "assignId", "foundry");
        Attribute attr = pFactory.newAttribute("http://www.w3.org/ns/prov#", "type", "prov", "doc-id-assignment",
                pFactory.newQualifiedName("http://www.w3.org/2001/XMLSchema", "string", "xsd"));
        final Activity activity = pFactory.newActivity(id3, factory.newXMLGregorianCalendar("2014-05-16T16:05:00"),
                factory.newXMLGregorianCalendar("2014-05-16T16:05:05"), Arrays.asList(attr));
        doc.getStatementOrBundle().add(activity);

        final WasAssociatedWith wasAssociatedWith = pFactory.newWasAssociatedWith(null, id3, id2);

        final Used used = pFactory.newUsed(id3, qn);
        doc.getStatementOrBundle().add(used);
        doc.getStatementOrBundle().add(wasAssociatedWith);

        Namespace ns = Namespace.gatherNamespaces(doc);
        doc.setNamespace(ns);


        final Converter convert = new Converter(pFactory);

        convert.writeDocument(doc, "/tmp/test_prov.json");
    }


    public void testProvenanceRecBuilder() throws Exception {
        ProvenanceRec.Builder builder = new ProvenanceRec.Builder("http://example.org", "foundry");
        //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        //sdf.setTimeZone(TimeZone.getDefault());
        //System.out.println(sdf.format(new Date()));

        ProvenanceRec provenanceRec = builder.docEntity("doc1", "document")
                .softwareAgent("docIdAssigner")
                .activity("assignId", "doc-id-assigment", "2014-05-16T16:05:00",
                        "2014-05-16T16:05:01")
                .wasAssociatedWith("assignId", "docIdAssigner")
                .used("assignId", "doc1")
                .build();

        System.out.println(provenanceRec.asJSON());
        provenanceRec.save("/tmp/doc_id_assigment_prov.json");


        ProvJSONValidator validator = new ProvJSONValidator();


        boolean ok = validator.validate("/tmp/doc_id_assigment_prov.json");
        assertTrue(ok);

        // test provenance update, attributes and automatic id generation

        JSONObject json = JSONUtils.loadFromFile("/tmp/doc_id_assigment_prov.json");
        builder = new ProvenanceRec.Builder("http://example.org", "foundry", json);
        String docId = builder.entityWithAttr("batchId=20140528", "sourceId=nlx_152590").getLastGeneratedId();
        String agentId = builder.softwareAgentWithAttr("name=DocIngestorCLI").getLastGeneratedId();
        String activityId = builder.activityWithAttr("ingest-docs", "2014-05-29T16:05:00",
                "2014-05-29T16:05:01").getLastGeneratedId();
        final ProvenanceRec prov2 = builder.wasAssociatedWith(activityId, agentId).used(activityId, docId).build();

        prov2.save("/tmp/doc_ingestion_prov.json");


    }

    public void testProvenanceRecBuilderMultiple() throws Exception {
        ProvenanceRec.Builder builder = new ProvenanceRec.Builder(
                "http://example.org", "foundry");

        String docId = builder.entityWithAttr("batchId=20140721", "sourceId=nlx_999998",
                "UUID=1234-4567-8978").getLastGeneratedId();

        String agentId = builder.softwareAgentWithAttr("name=DocIngestorCLI").getLastGeneratedId();
        String activityId = builder.activityWithAttr("ingest-docs", "2014-07-21T16:05:00",
                "2014-07-21T16:05:01").getLastGeneratedId();
        ProvenanceRec rec1 = builder.wasAssociatedWith(activityId, agentId).used(activityId, docId).build();

        rec1.save("/tmp/doc_ingestion_prov.json");

        builder = new ProvenanceRec.Builder("http://example.org", "foundry");
        docId = builder.entityWithAttr("batchId=20140721", "sourceId=nlx_999998", "UUID=1234-4567-8978").getLastGeneratedId();
        final ProvenanceRec provenanceRec = builder.softwareAgent("docIdAssigner")
                .activity("assignId", "doc-id-assigment", "2014-07-21T16:05:02",
                        "2014-07-21T16:05:03")
                .wasAssociatedWith("assignId", "docIdAssigner")
                .used("assignId", docId)
                .build();

        provenanceRec.save("/tmp/doc_id_assigment_prov.json");


        JSONObject json = JSONUtils.loadFromFile("/tmp/doc_ingestion_prov.json");
        builder = new ProvenanceRec.Builder("http://example.org", "foundry", json);

        ProvenanceRec prov2 = builder.softwareAgent("docIdAssigner")
                .activity("assignId", "doc-id-assigment", "2014-07-21T16:05:02",
                        "2014-07-21T16:05:03")
                .wasAssociatedWith("assignId", "docIdAssigner")
                .used("assignId", docId)
                .build();


        prov2.save("/tmp/doc_ingestion_id_assignment_combined_prov.json");
    }
}
