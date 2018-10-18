package org.neuinfo.foundry.common.util;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * Created by bozyurt on 7/8/15.
 */
public class ElasticSearchUtils {
    private final static Logger logger = Logger.getLogger(ElasticSearchUtils.class);

    public static boolean send2ElasticSearch(String jsonDocStr, String docId,
                                             String indexPath, String serverURL) throws Exception {
        return send2ElasticSearch(jsonDocStr, docId, indexPath, serverURL, null);
    }

    public static void sendBatch2ElasticSearch(Map<String, String> docId2JsonDocStrMap,
                                               String indexPath, String serverURL, String apiKey) throws Exception {
        //HttpClient client = new DefaultHttpClient();
        HttpClient client = HttpClientBuilder.create().build();
        URIBuilder builder = new URIBuilder(serverURL);
        builder.setPath("/_bulk");
        if (apiKey != null) {
            builder.setParameter("apikey", apiKey);
        }
        StringBuilder sb = new StringBuilder(64000);

        indexPath = ensureIndexPathStartsWithSlash(indexPath);
        String[] tokens = indexPath.split("/");
        Assertion.assertEquals(tokens.length, 3);
        String indexName = tokens[1];
        String type = tokens[2];
        for (String docId : docId2JsonDocStrMap.keySet()) {
            String jsonDocStr = docId2JsonDocStrMap.get(docId);
            sb.append("{\"create\":{\"_index\":\"").append(indexName).append("\", \"_type\":\"");
            sb.append(type).append("\",\"_id\":\"").append(docId).append("\"}}").append('\n');
            sb.append(jsonDocStr).append('\n');
        }

        URI uri = builder.build();
        System.out.println("uri:" + uri);
        HttpPost httpPost = new HttpPost(uri);
        try {
            httpPost.addHeader("Content-Type", "application/octet-stream");
            StringEntity entity = new StringEntity(sb.toString(), "UTF-8");
            httpPost.setEntity(entity);
            HttpResponse response = client.execute(httpPost);
            // logger.info(response.getStatusLine());
            logger.info(response);
            if (logger.isDebugEnabled() || response.getStatusLine().getStatusCode() != 200) {
                logger.info(EntityUtils.toString(response.getEntity()));
            }

        } finally {
            if (httpPost != null) {
                httpPost.releaseConnection();
            }
        }
    }

    public static boolean send2ElasticSearch(String jsonDocStr, String docId,
                                             String indexPath, String serverURL, String apiKey) throws Exception {
        //HttpClient client = new DefaultHttpClient();
        HttpClient client = HttpClientBuilder.create().build();

        URIBuilder builder = new URIBuilder(serverURL);
        // "http://localhost:9200/");
        indexPath = ensureIndexPathStartsWithSlash(indexPath);
        builder.setPath(indexPath + "/" + docId);  //"/nif/cinergi/" + docId);
        if (apiKey != null) {
            builder.setParameter("apikey", apiKey);
        }
        URI uri = builder.build();
        System.out.println("uri:" + uri);
        return sendPutRequest(jsonDocStr, client, uri);
    }

    public static boolean sendPutRequest(String jsonDocStr, HttpClient client, URI uri) throws IOException {
        HttpPut httpPut = new HttpPut(uri);
        boolean ok = false;
        try {
            httpPut.addHeader("Accept", "application/json");
            httpPut.addHeader("Content-Type", "application/json");
            StringEntity entity = new StringEntity(jsonDocStr, "UTF-8");
            httpPut.setEntity(entity);
            final HttpResponse response = client.execute(httpPut);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200 || statusCode == 201) {
                ok = true;
            } else {
                System.out.println(response.getStatusLine());
            }

        } finally {
            if (httpPut != null) {
                httpPut.releaseConnection();
            }
        }
        return ok;
    }

    public static String ensureIndexPathStartsWithSlash(String indexPath) {
        if (!indexPath.startsWith("/")) {
            indexPath = "/" + indexPath;
        }
        return indexPath;
    }

    public static boolean sendDeleteRequest(HttpClient client, URI uri) throws IOException {
        HttpDelete httpDelete = new HttpDelete(uri);
        boolean ok = false;
        try {
            final HttpResponse response = client.execute(httpDelete);
            if (response.getStatusLine().getStatusCode() == 200) {
                ok = true;
            }
        } finally {
            if (httpDelete != null) {
                httpDelete.releaseConnection();
            }
        }
        return ok;
    }
}
