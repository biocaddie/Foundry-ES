package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.consumers.plugin.Ingestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.net.URI;
import java.util.Map;

/**
 * Created by bozyurt on 7/24/15.
 */
public class WebServiceIngestor implements Ingestor {
    Map<String, String> optionMap;
    String serviceUrlTemplate;
    String sourceId;
    String dataSource;
    String returnType;
    boolean largeRecords = false;
    int numOfRecords = -1;
    int maxNumDocs2Ingest = -1;
    int count = 0;
    static Logger log = Logger.getLogger(WebServiceIngestor.class);

    @Override
    public void initialize(Map<String, String> options) throws Exception {
        this.serviceUrlTemplate = options.get("serviceUrlTemplate");
        this.sourceId = options.get("sourceId");
        if (options.containsKey("dataSource")) {
            this.dataSource = options.get("dataSource");
        }
        this.largeRecords = options.containsKey("largeRecords")
                ? Boolean.parseBoolean(options.get("largeRecords")) : false;
        this.returnType = options.get("returnType");
        Assertion.assertTrue(this.returnType.equals("json") || this.returnType.equals("xml"));
        this.optionMap = options;
    }

    @Override
    public void startup() throws Exception {

    }

    public String getContent(String ingestURL) throws Exception {
        //HttpClient client = new DefaultHttpClient();
        HttpClient client = HttpClientBuilder.create().build();
        URIBuilder builder = new URIBuilder(ingestURL);
        URI uri = builder.build();
        HttpGet httpGet = new HttpGet(uri);
        try {
            HttpResponse response = client.execute(httpGet);
            if (response.getStatusLine().getStatusCode() == 200) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    return EntityUtils.toString(entity);
                }
            }

        } finally {
            if (httpGet != null) {
                httpGet.releaseConnection();
            }
        }
        return null;
    }

    @Override
    public Result prepPayload() {
        return null;
    }

    @Override
    public String getName() {
        return "WebServiceIngestor";
    }

    @Override
    public int getNumRecords() {
        return 0;
    }

    @Override
    public String getOption(String optionName) {
        return optionMap.get(optionName);
    }

    @Override
    public void shutdown() {

    }

    @Override
    public boolean hasNext() {
        return false;
    }


}
