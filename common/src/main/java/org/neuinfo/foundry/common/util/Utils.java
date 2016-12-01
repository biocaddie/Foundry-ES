package org.neuinfo.foundry.common.util;

import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;
import com.mongodb.util.StringParseUtil;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.neuinfo.foundry.common.transform.Result;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * Created by bozyurt on 4/4/14
 */
public class Utils {


    public static Date parseDate(String currentValue, String dateFormat) {
        if (dateFormat == null || dateFormat.endsWith("Z")) {
            try {
                Calendar calendar = DatatypeConverter.parseDate(currentValue);
                return calendar.getTime();
            } catch (IllegalArgumentException x) {
                System.err.println(x.getMessage());
                // x.printStackTrace();
            }
        }
        if (dateFormat != null) {
            SimpleDateFormat origDateFormat = new SimpleDateFormat(dateFormat);
            try {
                Date date = origDateFormat.parse(currentValue);
                return date;
            } catch (ParseException x) {
                System.err.println(x.getMessage());
                // x.printStackTrace();
            }
        }
        // as last resort try natural language date parsing
        return  Utils.extractDate(currentValue);

    }
    public static Date extractDate(String freeFromDateStr) {
        Parser parser = new Parser();
        List<DateGroup> groups = parser.parse(freeFromDateStr);
        if (!groups.isEmpty()) {
            List<Date> dates = groups.get(0).getDates();
            if (!dates.isEmpty()) {
                return dates.get(0);
            }
        }
        return null;
    }

    public static int numOfCharsIn(String s, char c) {
        int count = 0, len = s.length();
        for (int i = 0; i < len; i++) {
            if (s.charAt(i) == c) {
                count++;
            }
        }
        return count;
    }

    public static Properties loadProperties(String propsFilename)
            throws IOException {
        InputStream is = Utils.class.getClassLoader().getResourceAsStream(
                propsFilename);
        if (is == null) {
            throw new IOException(
                    "Cannot find properties file in the classpath:"
                            + propsFilename
            );
        }
        Properties props = new Properties();
        props.load(is);

        return props;
    }

    public static String getStringValue(Object o, String defaultVal) {
        if (o == null) {
            return defaultVal;
        }
        return o.toString();
    }

    public static int getIntValue(Object o, int defaultVal) {
        if (o == null) {
            return defaultVal;
        }
        return Integer.parseInt(o.toString());
    }

    public static boolean getBoolValue(Object o, boolean defaultVal) {
        if (o == null) {
            return defaultVal;
        }
        return Boolean.parseBoolean(o.toString());
    }

    public static long getLongValue(Object o, long defaultVal) {
        if (o == null) {
            return defaultVal;
        }
        return Long.parseLong(o.toString());
    }

    public static void close(Reader in) {
        try {
            in.close();
        } catch (Exception x) {
            // ignore
        }
    }

    public static void close(Writer out) {
        try {
            out.close();
        } catch (Exception x) {
            // ignore
        }
    }

    public static void close(InputStream in) {
        try {
            in.close();
        } catch (Exception x) {
            // ignore
        }
    }

    public static void close(OutputStream os) {
        try {
            os.close();
        } catch (Exception x) {
            // no op
        }
    }

    public static BufferedReader newUTF8CharSetReader(String filename)
            throws IOException {
        return new BufferedReader(new InputStreamReader(new FileInputStream(
                filename), Charset.forName("UTF-8")));
    }

    public static BufferedWriter newUTF8CharSetWriter(String filename) throws IOException {
        return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename),
                Charset.forName("UTF-8")));
    }

    public static void saveXML(Element rootElem, String filename) throws Exception {
        BufferedWriter out = null;
        try {
            out = newUTF8CharSetWriter(filename);
            XMLOutputter xout = new XMLOutputter(Format.getPrettyFormat());
            xout.output(rootElem, out);
        } finally {
            close(out);
        }
    }

    public static String xmlAsString(Element rootElem) throws Exception {
        XMLOutputter xout = new XMLOutputter(Format.getPrettyFormat());
        StringWriter sw = new StringWriter(15000);
        xout.output(rootElem, sw);
        return sw.toString();
    }


    public static void extractGzippedFile(String gzippedFile, File destFile) throws Exception {
        BufferedInputStream in = null;
        BufferedOutputStream out = null;
        try {
            in = new BufferedInputStream(new GZIPInputStream(new FileInputStream(gzippedFile)));
            out = new BufferedOutputStream(new FileOutputStream(destFile));
            byte[] buffer = new byte[4096];
            int readBytes = 0;
            while ((readBytes = in.read(buffer)) != -1) {
                out.write(buffer, 0, readBytes);
            }
        } finally {
            close(in);
            close(out);
        }
    }

    public static String fromURL2FileName(String urlStr) {
        String filename = urlStr.replaceFirst("https?://|ftp://", "");
        return filename.replaceAll("[\\./\\(\\)]", "_");
    }

    public static File fromFile2Dir(File file) {
        String dirName = file.getName().replaceAll("\\.[^\\.]+$", "_out");
        if (dirName.equals(file.getName())) {
            dirName = file.getName() + "_out";
        }
        return new File(file.getParentFile(), dirName);
    }

    public static Element loadGzippedXML(String gzippedXmlFile) throws Exception {
        SAXBuilder builder = new SAXBuilder();
        InputStream in = null;
        Element root = null;
        try {
            in = new BufferedInputStream(new GZIPInputStream(new FileInputStream(gzippedXmlFile)));
            Document doc = builder.build(in);
            root = doc.getRootElement();
        } finally {
            close(in);
        }
        return root;
    }


    public static Element extractXMLFromTar(String tarFilePath, String xmlNamePattern) throws Exception {
        File tarFile = new File(tarFilePath);
        Assertion.assertTrue(tarFile.isFile());
        File destDir = null;
        try {
            destDir = createTempDirectory();
            Unpacker unpacker = new Unpacker(tarFile, destDir);
            unpacker.unpack();
            List<File> xmlFiles = Utils.findAllFilesMatching(destDir,
                    new RegexFileNameFilter(xmlNamePattern));
            if (!xmlFiles.isEmpty()) {
                return loadXML(xmlFiles.get(0).getAbsolutePath());
            }

        } finally {
            if (destDir != null) {
                deleteRecursively(destDir);
            }
        }
        return null;

    }


    public static void deleteRecursively(File dir) {
        if (dir.isFile()) {
            dir.delete();
        } else if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null && files.length > 0) {
                for (int i = 0; i < files.length; ++i) {
                    if (files[i].isDirectory()) {
                        deleteRecursively(files[i]);
                    } else {
                        files[i].delete();
                    }
                }
            }

            dir.delete();
        }
    }

    public static File createTempDirectory()
            throws IOException {
        final File temp = File.createTempFile("temp", Long.toString(System.nanoTime()));

        if (!(temp.delete())) {
            throw new IOException("Could not delete temp file: " + temp.getAbsolutePath());
        }

        if (!(temp.mkdir())) {
            throw new IOException("Could not create temp directory: " + temp.getAbsolutePath());
        }

        return temp;
    }

    public static Element loadXML(String xmlFile) throws Exception {
        SAXBuilder builder = new SAXBuilder();
        BufferedReader in = null;
        Element root = null;
        try {
            in = newUTF8CharSetReader(xmlFile);
            Document doc = builder.build(in);
            root = doc.getRootElement();
        } finally {
            close(in);
        }
        return root;
    }

    public static Element readXML(String xmlContent) throws Exception {
        SAXBuilder builder = new SAXBuilder();
        Document docEl = builder.build(new StringReader(xmlContent));
        return docEl.getRootElement();
    }

    public static String loadAsString(String textFile) throws IOException {
        StringBuilder buf = new StringBuilder((int) new File(textFile).length());
        BufferedReader in = null;
        try {
            in = newUTF8CharSetReader(textFile);

            String line;
            while ((line = in.readLine()) != null) {
                buf.append(line).append('\n');
            }
        } finally {
            close(in);
        }
        return buf.toString().trim();
    }

    public static void saveText(String text, String textFile) throws IOException {
        BufferedWriter out = null;
        try {
            out = newUTF8CharSetWriter(textFile);

            out.write(text);
            out.newLine();
        } finally {
            close(out);
        }
    }

    public static Properties loadPropertiesFromPath(String propsFilePath)
            throws IOException {
        InputStream is = null;
        try {
            is = new BufferedInputStream(new FileInputStream(propsFilePath));
            Properties props = new Properties();
            props.load(is);
            return props;
        } finally {
            close(is);
        }
    }

    public static boolean isEmpty(String s) {
        return s == null || s.trim().isEmpty();
    }

    public static String prepBatchId(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("MMddyyyy-HH:mm");
        return sdf.format(date);
    }


    public static String nextVersion(String version) {
        String[] tokens = version.split("\\.");
        String lastToken = tokens[tokens.length - 1];
        int curVersion = getIntValue(lastToken, -1);
        Assertion.assertTrue(curVersion != -1);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tokens.length - 1; i++) {
            sb.append(tokens[i]).append('.');
        }
        sb.append(curVersion + 1);
        return sb.toString();
    }

    public static String prepGetterName(String propertyName) {
        StringBuilder sb = new StringBuilder();
        sb.append("get").append(propertyName.substring(0, 1).toUpperCase()).append(propertyName.substring(1));
        return sb.toString();
    }

    public static String prepSetterName(String propertyName) {
        StringBuilder sb = new StringBuilder();
        sb.append("set").append(propertyName.substring(0, 1).toUpperCase()).append(propertyName.substring(1));
        return sb.toString();
    }

    public static String getMD5Checksum(String filePath) throws Exception {
        byte[] barr = createMD5Checksum(filePath);
        StringBuilder sb = new StringBuilder(32);
        for (int i = 0; i < barr.length; i++) {
            sb.append(Integer.toString((barr[i] & 0xff) + 0x100, 16).substring(
                    1));
        }
        return sb.toString();
    }

    public static String getMD5ChecksumOfString(String text) throws Exception {
        byte[] barr = createMD5ChecksumOfString(text);
        StringBuilder sb = new StringBuilder(32);
        for (int i = 0; i < barr.length; i++) {
            sb.append(Integer.toString((barr[i] & 0xff) + 0x100, 16).substring(
                    1));
        }
        return sb.toString();
    }

    public static byte[] createMD5ChecksumOfString(String text)
            throws Exception {
        byte[] buffer = text.getBytes("UTF-8");
        MessageDigest md = MessageDigest.getInstance("MD5");
        return md.digest(buffer);
    }

    public static byte[] createMD5Checksum(String filePath) throws Exception {
        BufferedInputStream in = null;
        byte[] buffer = new byte[4096];
        MessageDigest md = MessageDigest.getInstance("MD5");
        try {
            int nr;
            in = new BufferedInputStream(new FileInputStream(filePath));
            while ((nr = in.read(buffer)) > 0) {
                md.update(buffer, 0, nr);
            }
            return md.digest();
        } finally {
            close(in);
        }
    }

    public static boolean areFilesSame(File file1, File file2) throws Exception {
        if (file1.length() != file2.length()) {
            return false;
        }
        String checksum1 = getMD5Checksum(file1.getAbsolutePath());
        String checksum2 = getMD5Checksum(file2.getAbsolutePath());
        return checksum1.equals(checksum2);

    }

    public static List<File> findAllFilesMatching(File rootDir, FilenameFilter filter) {
        List<File> filteredFiles = new LinkedList<File>();
        findAllFilesMatching(rootDir, filter, filteredFiles);
        return filteredFiles;
    }

    static void findAllFilesMatching(File parent, FilenameFilter filter, List<File> filteredFiles) {
        if (parent.isDirectory()) {
            for (File f : parent.listFiles()) {
                findAllFilesMatching(f, filter, filteredFiles);
            }
        } else {
            if (filter == null) {
                filteredFiles.add(parent);
            } else {
                if (filter.accept(parent.getParentFile(), parent.getName())) {
                    filteredFiles.add(parent);
                }
            }
        }
    }

    public static boolean isNumber(Object value) {
        if (value == null) {
            return false;
        }
        try {
            Double.parseDouble(value.toString());
            return true;
        } catch (NumberFormatException nfe) {
            return false;
        }
    }

    public static Double toDouble(String value) {
        try {
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException nfe) {
            return null;
        }
    }

    public static String join(List<String> values, String delim) {
        StringBuilder sb = new StringBuilder(values.size() * 11);
        for (Iterator<String> it = values.iterator(); it.hasNext(); ) {
            String value = it.next();
            sb.append(value);
            if (it.hasNext()) {
                sb.append(delim);
            }
        }
        return sb.toString();
    }

    public static class RegexFileNameFilter implements FilenameFilter {
        Pattern p;

        public RegexFileNameFilter(String regexPattern) {
            p = Pattern.compile(regexPattern);
        }

        @Override
        public boolean accept(File dir, String name) {
            return p.matcher(name).find();
        }
    }


    public static String[] splitServerURLAndPath(String urlStr) throws MalformedURLException {
        URL url = new URL(urlStr);
        String[] toks = new String[2];
        String path = url.getPath();
        int idx = urlStr.indexOf(path);
        Assertion.assertTrue(idx != -1);
        toks[0] = urlStr.substring(0, idx);
        toks[1] = path;
        return toks;
    }

    public static final int sizeOf(Object obj) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos;
        try {
            oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            oos.close();
            byte[] bytes = bos.toByteArray();
            return bytes != null ? bytes.length : 0;
        } catch (Exception x) {
            x.printStackTrace();
        }
        return -1;
    }


    public static File getContentFromURL(String ingestURL) throws URISyntaxException, IOException {
        HttpClient client = new DefaultHttpClient();
        URIBuilder builder = new URIBuilder(ingestURL);
        URI uri = builder.build();
        HttpGet httpGet = new HttpGet(uri);
        try {
            HttpResponse response = client.execute(httpGet);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                String xmlStr = EntityUtils.toString(entity);
                File tempFile = File.createTempFile("uri_", "_content");
                Utils.saveText(xmlStr, tempFile.getAbsolutePath());
                return tempFile;
            }
        } finally {
            if (httpGet != null) {
                httpGet.releaseConnection();
            }
        }
        return null;
    }

    public static String sendGetRequest(String ingestURL) throws URISyntaxException, IOException {
        HttpClient client = new DefaultHttpClient();
        URIBuilder builder = new URIBuilder(ingestURL);
        URI uri = builder.build();
        HttpGet httpGet = new HttpGet(uri);
        try {
            HttpResponse response = client.execute(httpGet);
            HttpEntity entity = response.getEntity();
            if (entity != null && response.getStatusLine().getStatusCode() == 200) {
                String xmlStr = EntityUtils.toString(entity);
                return xmlStr;
            } else if (response.getStatusLine().getStatusCode() == 503) {
                throw new RuntimeException("503");
            }

        } finally {
            if (httpGet != null) {
                httpGet.releaseConnection();
            }
        }
        return null;
    }

    public static String loadTextFromClasspath(String textFilePath) {
        InputStream in = null;
        try {
            in = Utils.class.getClassLoader().getResourceAsStream(textFilePath);
            Scanner scanner = new Scanner(in).useDelimiter("\\A");
            return scanner.hasNext() ? scanner.next() : "";
        } finally {
            close(in);
        }
    }


    public static boolean handleLike(String value, String refValue) {
        List<Integer> indices = new ArrayList<Integer>(5);
        char[] chars = refValue.toCharArray();
        int i = 0, len = chars.length;
        while (i < len) {
            if (chars[i] == '%') {
                if (i + 1 >= len || chars[i + 1] != '%') {
                    indices.add(i);
                    i++;
                } else {
                    i += 2;
                }
            } else {
                i++;
            }
        }
        if (indices.size() == 1) {
            int idx = indices.get(0);
            if (idx == 0) {
                String suffix = refValue.substring(1).replaceAll("%%", "%");
                return value.endsWith(suffix);
            } else if (idx + 1 == len) {
                String prefix = refValue.substring(0, refValue.length() - 1).replaceAll("%%", "%");
                return value.startsWith(prefix);
            }
        }
        StringBuilder sb = new StringBuilder(128);
        int offset = 0;
        for (Integer idx : indices) {
            String s = refValue.substring(offset, idx);
            if (s.length() > 0) {
                sb.append(s.replaceAll("%%", "%"));
            }
            sb.append(".*");
            offset = idx + 1;
        }
        if (offset + 1 < len) {
            sb.append(refValue.substring(offset).replaceAll("%%", "%"));
        }
        Pattern p = Pattern.compile(sb.toString());

        Matcher matcher = p.matcher(value);
        return matcher.find();
    }

    public static void stream2File(String fromClassPath, String outPath) throws IOException {
        BufferedReader bin = null;
        BufferedWriter out = null;
        try {
            out = Utils.newUTF8CharSetWriter(outPath);
            bin = new BufferedReader(
                    new InputStreamReader(Utils.class.getClassLoader().getResourceAsStream(fromClassPath), Charset.forName("UTF-8")));
            String line;
            while( (line = bin.readLine())  != null) {
                out.write(line);
                out.newLine();
            }

        } finally {
            Utils.close(bin);
            Utils.close(out);
        }
    }

    public static void main(String[] args) throws Exception {

        System.out.println(handleLike("abab", "%ba%"));
        System.out.println(handleLike("abab", "ab%"));
        System.out.println(handleLike("abab", "%ab"));
        System.out.println(handleLike("abab", "%c"));
        System.out.println(handleLike("ab%ab", "%%%ab"));
    }

}
