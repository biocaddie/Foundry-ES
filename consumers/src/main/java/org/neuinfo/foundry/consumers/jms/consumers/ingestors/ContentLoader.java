package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.common.Constants;
import org.neuinfo.foundry.consumers.common.Parameters;

import java.io.*;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Created by bozyurt on 10/21/15.
 */
public class ContentLoader {
    static Logger logger = Logger.getLogger(ContentLoader.class);
    String offsetParam;
    int curOffset = 0;
    int pageIdx = 1;

    private ContentLoader() {
    }

    public ContentLoader(String offsetParam) {
        this.offsetParam = offsetParam;
    }


    public File getNextContentPage(String ingestURL, String cacheFileName, String limitParam, int limitValue, boolean useCache) throws Exception {
        URIBuilder uriBuilder = new URIBuilder(ingestURL);
        uriBuilder.addParameter(offsetParam, String.valueOf(curOffset));
        if (limitParam != null) {
            uriBuilder.addParameter(limitParam, String.valueOf(limitValue));
        }
        String url = uriBuilder.build().toString();
        System.out.println("getting content from " + url);
        String cachePageFile = cacheFileName + "_" + pageIdx;
        File f = getContent(url, cachePageFile, useCache);
        ++pageIdx;
        return f;
    }

    public void incrOffset(int incrAmount) {
        curOffset += incrAmount;
    }

    public static File getContent(String ingestURL) throws Exception {
        return getContent(ingestURL, null, false);
    }

    public static File getContent(String ingestURL, String cacheFileName, boolean useCache) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("getContent for " + ingestURL);
        }

        boolean gzippedFile = ingestURL.endsWith(".gz");
        Parameters parameters = Parameters.getInstance();
        String cacheRoot = parameters.getParam("cache.root");
        if (ingestURL.startsWith("file://")) {
            String filePath = ingestURL.replaceFirst("file://", "");
            if (cacheFileName == null) {
                cacheFileName = filePath;
                cacheFileName = cacheFileName.replaceAll("[\\./\\(\\)]", "_");
            }
            File cacheFile = new File(cacheRoot, cacheFileName);
            if (cacheFile.isFile()) {
                return cacheFile;
            }
            return getFile(cacheFile, gzippedFile, new FileInputStream(new File(filePath)));
        } else {
            // remote file
            if (cacheFileName == null) {
                cacheFileName = Utils.fromURL2FileName(ingestURL);
            }
            File cacheFile = new File(cacheRoot, cacheFileName);
            if (cacheFile.isFile()) {
                if (!useCache) {
                    cacheFile.delete();
                } else {
                    return cacheFile;
                }
            }
            HttpClient client = new DefaultHttpClient();
            URIBuilder builder = new URIBuilder(ingestURL);
            URI uri = builder.build();
            HttpGet httpGet = new HttpGet(uri);
            try {
                HttpResponse response = client.execute(httpGet);
                HttpEntity entity = response.getEntity();
                if (entity != null) {
                    return getFile(cacheFile, gzippedFile, entity.getContent());
                }
            } finally {
                if (httpGet != null) {
                    httpGet.releaseConnection();
                }
            }
        }
        return null;
    }

    private static File getFile(File cacheFile, boolean gzippedFile, InputStream contentIn) throws IOException {
        if (gzippedFile) {
            BufferedInputStream in = null;
            BufferedOutputStream out = null;
            try {
                in = new BufferedInputStream(new GZIPInputStream(contentIn));
                out = new BufferedOutputStream(new FileOutputStream(cacheFile));
                byte[] buffer = new byte[4096];
                int readBytes = 0;
                while ((readBytes = in.read(buffer)) != -1) {
                    out.write(buffer, 0, readBytes);
                }
                return cacheFile;
            } finally {
                Utils.close(in);
                Utils.close(out);
            }
        } else {
            BufferedWriter out = null;
            BufferedReader in = null;
            try {
                in = new BufferedReader(new InputStreamReader(contentIn));
                out = Utils.newUTF8CharSetWriter(cacheFile.getAbsolutePath());
                String line;
                while ((line = in.readLine()) != null) {
                    out.write(line);
                    out.newLine();
                }
                return cacheFile;
            } finally {
                Utils.close(in);
                Utils.close(out);
            }
        }
    }

}
