package org.neuinfo.foundry.consumers.common;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.neuinfo.foundry.common.provenance.ProvenanceRec;

import java.net.URI;

/**
 * Created by bozyurt on 12/4/14.
 */
public class ProvenanceClient {
    private String serverURL = "http://geoprovdb.webfactional.com/";
    private String user = "cinergi";
    private String pwd = "4cinergi_prov";
//http://geoprovdb.webfactional.com/provdb/api/foundry/provenance/


    public void getProvenance(String docUUID) throws Exception {
        //HttpClient client = new DefaultHttpClient();
        HttpClient client = HttpClientBuilder.create().build();
        URIBuilder builder = new URIBuilder(serverURL).setPath("/provdb/api/foundry/provenance/" + docUUID);
        URI uri = builder.build();
        System.out.println("uri:" + uri);
        HttpGet httpGet = new HttpGet(uri);
        try {
            HttpResponse response = client.execute(httpGet);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                HttpEntity entity = response.getEntity();
                String jsonStr = EntityUtils.toString(entity);
                JSONObject json = new JSONObject(jsonStr);
                System.out.println(json.toString(2));
            } else {
                System.out.println(response.getStatusLine());
            }
        } finally {
            if (httpGet != null) {
                httpGet.releaseConnection();
            }
        }
    }

    public void deleteProvenance(String docUUID) throws Exception {
        //DefaultHttpClient client = new DefaultHttpClient();
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(user, pwd));
        HttpClient client = HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build();
        //client.getCredentialsProvider().setCredentials(AuthScope.ANY,
        //        new UsernamePasswordCredentials(user, pwd));
        URIBuilder builder = new URIBuilder(serverURL).setPath("/provdb/api/foundry/provenance/" + docUUID);
        URI uri = builder.build();
        System.out.println("uri:" + uri);
        HttpDelete httpDel = new HttpDelete(uri);
        try {
            final HttpResponse response = client.execute(httpDel);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                System.out.println(response.getStatusLine());
            }
        } finally {
            if (httpDel != null) {
                httpDel.releaseConnection();
            }
        }
    }

    public String saveProvenance(ProvenanceRec provRec) throws Exception {
        //DefaultHttpClient client = new DefaultHttpClient();
        //client.getCredentialsProvider().setCredentials(AuthScope.ANY,
        //        new UsernamePasswordCredentials(user, pwd));
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(user, pwd));
        HttpClient client = HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build();
        URIBuilder builder = new URIBuilder(serverURL).setPath("/provdb/api/provenance/");
        URI uri = builder.build();
        System.out.println("uri:" + uri);
        HttpPost httpPost = new HttpPost(uri);
        try {
            String jsonStr = provRec.asJSON();
            httpPost.addHeader("Accept", "application/json");
            httpPost.addHeader("Content-Type", "application/json");
            StringEntity entity = new StringEntity(jsonStr, "UTF-8");
            httpPost.setEntity(entity);
            final HttpResponse response = client.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200 || statusCode == 201) {
                HttpEntity entity1 = response.getEntity();
                String resultJsonStr = EntityUtils.toString(entity1);
                try {
                    JSONObject js = new JSONObject(resultJsonStr);
                    String requestId = js.getString("request id: ");
                    return requestId;
                } catch (Throwable t) {
                    t.printStackTrace();
                }
                System.out.println(resultJsonStr);
            } else {
                System.out.println(response);
                System.out.println(EntityUtils.toString(response.getEntity()));
            }
            return null;
        } finally {
            if (httpPost != null) {
                httpPost.releaseConnection();
            }
        }
    }


}
