package org.neuinfo.foundry.consumers.jms.consumers;

import org.jdom2.input.SAXBuilder;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.neuinfo.foundry.common.ingestion.DocumentIngestionService;
import org.neuinfo.foundry.common.ingestion.SourceConfig;
import org.neuinfo.foundry.common.model.Source;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.XML2JSONConverter;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by bozyurt on 10/1/14.
 */
public class SimpleIngestionConsumer extends JMSConsumerSupport implements MessageListener {

    public SimpleIngestionConsumer(String queueName) {
        super(queueName);
    }

    @Override
    public void onMessage(Message message) {
        try {
            ObjectMessage om = (ObjectMessage) message;
            String payload = (String) om.getObject();
            System.out.println("payload:" + payload);
            JSONObject json = new JSONObject(payload);
            String command = json.getString("cmd");
            String srcNifId = json.getString("srcNifId");
            String batchId = json.getString("batchId");
            String dataSource = json.getString("dataSource");
            System.out.println("received command '" + command + "' [srcNifId:"
                    + srcNifId + ", batchId:" + batchId + "]");
            //TODO handle ingestion

            List<String> documentList = getDocumentList("http://hydro10.sdsc.edu/metadata/NOAA_NGDC/");
            final List<String> docUrlList = documentList.subList(0, 10);
            documentList = null;
            for (String docUrl : docUrlList) {
                System.out.println(docUrl);
            }

            ingestWAFDocs(batchId, dataSource, srcNifId, docUrlList);
        } catch (Exception x) {
            //TODO proper error handling
            x.printStackTrace();
        }

    }

    void ingestWAFDocs(String batchId, String srcNifId, String dataSource, List<String> docUrlList) throws Exception {

        DocumentIngestionService dis = new DocumentIngestionService();
        try {
            dis.start(this.config);
            Source source = dis.findSource(srcNifId, dataSource);
            Assertion.assertNotNull(source, "Cannot find source for nifId:" + srcNifId);

            dis.setSource(source);
            int submittedCount = docUrlList.size();
            int ingestedCount = 0;
            dis.beginBatch(source, batchId);
            for (String docUrl : docUrlList) {
                final JSONObject json = prepNOAAJsonPayload(docUrl);
                System.out.println(json.toString(2));
                try {
                    dis.saveDocument(json, batchId, source, "new", "records");
                    ingestedCount++;
                } catch (Exception x) {
                    x.printStackTrace();
                }
                System.out.println("=============================================");

            }

            dis.endBatch(source, batchId, ingestedCount, submittedCount, 0);
        } finally {
            dis.shutdown();
        }

    }

    public static JSONObject prepNOAAJsonPayload(String xmlSource) throws Exception {
        SAXBuilder builder = new SAXBuilder();
        org.jdom2.Document doc = builder.build(xmlSource);
        org.jdom2.Element rootEl = doc.getRootElement();

        XML2JSONConverter converter = new XML2JSONConverter();
        final JSONObject json = converter.toJSON(rootEl);

        return json;
    }

    public static List<String> getDocumentList(String rootUrl) throws IOException {
        List<String> links = new ArrayList<String>();
        final Document doc = Jsoup.connect(rootUrl).get();
        final Elements anchorEls = doc.select("a");
        final Iterator<Element> it = anchorEls.iterator();
        while (it.hasNext()) {
            Element ae = it.next();
            final String href = ae.attr("abs:href");
            if (href != null && href.endsWith(".xml")) {
                links.add(href);
            }
        }
        return links;
    }

    @Override
    public String getId() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }
}
