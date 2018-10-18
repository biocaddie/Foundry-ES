package org.neuinfo.foundry.consumers.common;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.jdom2.Element;
import org.json.JSONObject;
import org.neuinfo.foundry.common.util.ElasticSearchUtils;
import org.neuinfo.foundry.common.util.JSONUtils;
import org.neuinfo.foundry.common.util.XML2JSONConverter;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.io.File;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

/**
 * Created by bozyurt on 11/20/14.
 */
public class ConsumerUtils {
    public static File prepareInputFile(String format, JSONObject originalDoc, String objectId) throws Exception {
        File dir = new File("/tmp/consumers");
        if (!dir.isDirectory()) {
            dir.mkdirs();
        }

        File inputFile;
        if (format.equals("xml")) {
            inputFile = new File(dir, objectId + ".xml");
            XML2JSONConverter converter = new XML2JSONConverter();
            // System.out.println(originalDoc.toString(2));
            Element docEl = converter.toXML(originalDoc);
            org.neuinfo.foundry.common.util.Utils.saveXML(docEl, inputFile.getAbsolutePath());
        } else {
            inputFile = new File(dir, objectId + ".json");
        }

        return inputFile;
    }

    public static File prepareOutFile(String format, String objectId) {
        File dir = new File("/tmp/consumers");
        if (!dir.isDirectory()) {
            dir.mkdirs();
        }
        File outputFile;
        if (format.equals("xml")) {
            outputFile = new File(dir, objectId + ".xml");
        } else {
            outputFile = new File(dir, objectId + ".json");
        }
        return outputFile;
    }

    public static void sendBatch2ElasticSearch(Map<String, String> docId2JsonDocStrMap,
                                               String indexPath, String serverURL) throws Exception {
        //HttpClient client = new DefaultHttpClient();
        HttpClient client = HttpClientBuilder.create().build();
        String[] tokens = indexPath.split("/");

        String indexName = tokens[1];
        String type = tokens[2];
        URIBuilder builder = new URIBuilder(serverURL);
        builder.setPort(9200);
        builder.setPath("/_bulk");
        StringBuilder sb = new StringBuilder(64000);
        for (Map.Entry<String, String> entry : docId2JsonDocStrMap.entrySet()) {

            String docId = entry.getKey();
            String jsonDocStr = entry.getValue();
            sb.append("{\"create\":{\"_index\":\"").append(indexName).append("\", \"_type\":\"");
            sb.append(type).append("\",\"_id\":").append(docId).append("\"}}\n");
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
            System.out.println(response.getStatusLine());
            System.out.println(EntityUtils.toString(response.getEntity()));
        } finally {
            if (httpPost != null) {
                httpPost.releaseConnection();
            }
        }
    }

    public static boolean send2ElasticSearch(String jsonDocStr, String docId,
                                             String indexPath, String serverURL) throws Exception {
        //HttpClient client = new DefaultHttpClient();
        HttpClient client = HttpClientBuilder.create().build();

        URIBuilder builder = new URIBuilder(serverURL);
        // "http://localhost:9200/");
        indexPath = ensureIndexPathStartsWithSlash(indexPath);
        builder.setPath(indexPath + "/" + docId);  //"/nif/cinergi/" + docId);
        URI uri = builder.build();
        System.out.println("uri:" + uri);
        return ElasticSearchUtils.sendPutRequest(jsonDocStr, client, uri);
    }


    public static String ensureIndexPathStartsWithSlash(String indexPath) {
        if (!indexPath.startsWith("/")) {
            indexPath = "/" + indexPath;
        }
        return indexPath;
    }


    public static String getTimeInProvenanceFormat() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
        sdf.setTimeZone(TimeZone.getDefault());
        return sdf.format(new Date());
    }

    public static String getTimeInProvenanceFormat(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
        sdf.setTimeZone(TimeZone.getDefault());
        return sdf.format(date);
    }

    public static Result convert2JSON(Element el) throws Throwable {
        return convert2JSON(el, false);
    }

    public static Result convert2JSON(Element el, boolean normalize) throws Throwable {
        XML2JSONConverter converter = new XML2JSONConverter();
        JSONObject json = converter.toJSON(el);
        if (normalize) {
            JSONUtils.normalize(json);
        }
        return new Result(json, Result.Status.OK_WITH_CHANGE);
    }

    public static Result toErrorResult(Throwable t, Logger log) {
        log.error("prepPayload", t);
        t.printStackTrace();
        return new Result(null, Result.Status.ERROR, t.getMessage());
    }

    public static void main(String[] args) {
        System.out.println(getTimeInProvenanceFormat());
    }
}
