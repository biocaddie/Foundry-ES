package org.neuinfo.foundry.common.util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.VocabularyInfo;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by bozyurt on 3/10/16.
 */
public class SynonymsRequestManager {
    private String serverURL = "http://matrix.neuinfo.org:9000";
    private static Map<String, VocabularyInfo> cache = Collections.synchronizedMap(new LRUCache<String, VocabularyInfo>(1000));
    private AtomicBoolean verbose = new AtomicBoolean(false);
    private final static Logger log = Logger.getLogger(SynonymsRequestManager.class);
    private static SynonymsRequestManager instance = null;

    public synchronized static SynonymsRequestManager getInstance() {
        if (instance == null) {
            instance = new SynonymsRequestManager();
        }
        return instance;
    }

    private SynonymsRequestManager() {
    }

    public VocabularyInfo getSynonyms(String identifier) throws Exception {
        VocabularyInfo vi = cache.get(identifier);
        if (vi != null) {
            return vi;
        }
        HttpClient client = new DefaultHttpClient();
        URIBuilder builder = new URIBuilder(serverURL);
        builder.setPath("/scigraph/vocabulary/id/" + identifier);
        URI uri = builder.build();
        log.info("getSynonyms:: uri:" + uri);
        HttpGet httpGet = new HttpGet(uri);
        try {
            httpGet.addHeader("Accept", "application/json");
            HttpResponse response = client.execute(httpGet);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    String jsonStr = EntityUtils.toString(entity);
                    JSONObject json = new JSONObject(jsonStr);
                    if (verbose.get()) {
                        System.out.println(json.toString(2));
                    }
                    vi = VocabularyInfo.fromJSON(identifier, json);
                    cache.put(identifier, vi);
                    return vi;
                }
            }
        } finally {
            if (httpGet != null) {
                httpGet.releaseConnection();
            }
        }
        return null;
    }

    public AtomicBoolean getVerbose() {
        return verbose;
    }

    public void setVerbose(boolean verboseValue) {
        this.verbose.set(verboseValue);
    }

    public static void main(String[] args) throws Exception {
        SynonymsRequestManager srm = new SynonymsRequestManager();
        VocabularyInfo vi = srm.getSynonyms("UBERON:0002421");

        System.out.println(vi);
    }
}
