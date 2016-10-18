package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import org.apache.log4j.Logger;
import org.jdom2.Element;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.common.*;
import org.neuinfo.foundry.consumers.plugin.*;

import java.io.File;
import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bozyurt on 4/18/16.
 */
public class PubMedIngestor implements Ingestor, IngestorLifeCycle {
    String ingestURL;
    String opMode = "full"; // or update
    String updateURL;
    String topElName;
    String docElName;
    String fileNamePattern;
    String remotePath;
    String ftpHost;
    boolean useCache = true;
    Map<String, String> optionMap;
    boolean sampleMode = false;
    int sampleSize = 1;
    XMLFileIterator xmlFileIterator;
    Map<String, DeleteInfo> diMap = new HashMap<String, DeleteInfo>();
    static Logger log = Logger.getLogger(PubMedIngestor.class);

    @Override
    public void initialize(Map<String, String> options) throws Exception {
        this.optionMap = options;
        this.ingestURL = options.get("ingestURL");
        this.updateURL = options.get("updateURL");
        this.docElName = options.get("documentElement");
        this.topElName = options.containsKey("topElement") ? options.get("topElement") : null;
        this.fileNamePattern = options.get("filenamePattern");

        this.sampleMode = options.containsKey("sampleMode")
                ? Boolean.parseBoolean(options.get("sampleMode")) : false;

        this.sampleSize = Utils.getIntValue(options.get("sampleSize"), 1);
        URL url;
        if (opMode.equals("full")) {
            url = new URL(this.ingestURL);
        } else {
            Assertion.assertNotNull(this.updateURL, "updateURL");
            url = new URL(this.updateURL);
            this.optionMap.put("docUpdaterImplClass", "org.neuinfo.foundry.consumers.jms.consumers.ingestors.PubmedDocModifier");
        }
        this.ftpHost = url.getHost();
        this.remotePath = url.getPath();


    }

    @Override
    public void startup() throws Exception {
        final Parameters parameters = Parameters.getInstance();
        String cacheRoot = parameters.getParam("cache.root");

        List<FTPUtils.FileInfo> filesMatching = getFilesMatching(this.fileNamePattern);
        String topDirName;
        if (opMode.equals("full")) {
            topDirName = Utils.fromURL2FileName(ingestURL);
        } else {
            topDirName = Utils.fromURL2FileName(updateURL);
        }
        File outDir = new File(cacheRoot, topDirName);
        outDir.mkdirs();
        FtpClient client = new FtpClient(ftpHost);
        List<File> localFiles = new ArrayList<File>(filesMatching.size());
        int i = 0;
        for (FTPUtils.FileInfo fi : filesMatching) {
            String remoteFilePath = fi.getFilePath();
            Assertion.assertTrue(remoteFilePath.startsWith(remotePath));
            String relativePath = remoteFilePath.substring(remotePath.length());
            File localFile = new File(outDir, relativePath);
            if (!localFile.getParentFile().isDirectory()) {
                localFile.getParentFile().mkdirs();
            }
            // check if file is already cached and same size as the remote file. If so use the local file
            if (!localFile.isFile() || localFile.length() != fi.getSize()) {
                System.out.println("transferring file:" + remoteFilePath);
                //URL remoteURL = new URL("ftp", ftpHost, 80, remoteFilePath);
                //System.out.println("remoteURL:" + remoteURL);
                client.transferFile(localFile.getAbsolutePath(), remoteFilePath, true);
            }
            localFiles.add(localFile);
            i++;
            if (!canContinue(i)) {
                break;
            }
        }

        final Pattern pattern = Pattern.compile("(\\d+)\\.xml\\.gz$");
        Collections.sort(localFiles, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                Matcher m1 = pattern.matcher(o1.getName());
                Matcher m2 = pattern.matcher(o2.getName());
                if (m1.find() && m2.find()) {
                    int num1 = Utils.getIntValue(m1.group(1), -1);
                    int num2 = Utils.getIntValue(m2.group(1), -1);
                    return num1 - num2;
                }
                return 0;
            }
        });
        RemoteFileIterator rfi = new RemoteFileIterator(localFiles);
        this.xmlFileIterator = new XMLFileIterator(rfi, this.topElName, this.docElName);
        if (opMode.equals("update")) {
            prepareDeleteInfos(localFiles);
        }
    }


    void prepareDeleteInfos(List<File> localFiles) throws Exception {
        RemoteFileIterator rfi = new RemoteFileIterator(localFiles);
        XMLFileIterator iterator = new XMLFileIterator(rfi, this.topElName, "DeleteCitation");
        while (iterator.hasNext()) {
            Element element = iterator.next();
            List<Element> pmidEls = element.getChildren("PMID");
            File curFile = iterator.getCurFile();
            String filename = curFile.getName();
            for (Element pmidEl : pmidEls) {
                String pmid = pmidEl.getTextTrim();
                DeleteInfo di = new DeleteInfo(pmid, filename);
                diMap.put(pmid, di);
            }
        }
    }


    static class DeleteInfo {
        private final String pmid;
        private final String fileName;

        public DeleteInfo(String pmid, String fileName) {
            this.pmid = pmid;
            this.fileName = fileName;
        }

        public String getPmid() {
            return pmid;
        }

        public String getFileName() {
            return fileName;
        }
    }

    public List<FTPUtils.FileInfo> getFilesMatching(String fnamePattern) throws Exception {
        String remoteDir = this.remotePath;
        FtpClient client = new FtpClient(ftpHost);
        Pattern pattern = Pattern.compile(fnamePattern);
        List<FTPUtils.FileInfo> filteredFiles = new LinkedList<FTPUtils.FileInfo>();
        FTPUtils.recurseRemoteDirs(remoteDir, client, pattern, filteredFiles,
                sampleMode, sampleSize, false, 10);
        return filteredFiles;
    }

    @Deprecated
    void startupOld() throws Exception {
        Parameters parameters = Parameters.getInstance();
        String cacheRoot = parameters.getParam("cache.root");
        if (opMode.equals("full")) {


            LFTPWrapper lftp = new LFTPWrapper(ingestURL, this.fileNamePattern);
            String topDirName = Utils.fromURL2FileName(ingestURL);
            if (sampleMode) {
                File sampleCacheRoot = new File(cacheRoot, "sample");
                sampleCacheRoot.mkdirs();
                File outDir = new File(sampleCacheRoot, topDirName);
                outDir.mkdirs();
                List<File> sample = lftp.sample(1, outDir.getAbsolutePath());
                this.xmlFileIterator = new XMLFileIterator(new RemoteFileIterator(sample),
                        this.topElName, this.docElName);
            } else {
                File outDir = new File(cacheRoot, topDirName);
                outDir.mkdirs();

                List<File> files = lftp.mirror(this.fileNamePattern, outDir.getAbsolutePath());
                RemoteFileIterator rfi = new RemoteFileIterator(files);
                this.xmlFileIterator = new XMLFileIterator(rfi, this.topElName, this.docElName);
            }
        }
    }

    @Override
    public Result prepPayload() {
        try {
            Element element = xmlFileIterator.next();
            if (opMode.equals("update")) {
                String pmid = element.getChildTextTrim("PMID");
                if (diMap.containsKey(pmid)) {
                    DeleteInfo di = diMap.get(pmid);
                    Element elem = new Element("Deleted");
                    elem.setText(di.getFileName());
                    element.addContent(elem);
                    diMap.remove(pmid);
                }
            }
            Result r = ConsumerUtils.convert2JSON(element);
            return r;
        } catch (Throwable t) {
            return ConsumerUtils.toErrorResult(t, log);
        }
    }

    @Override
    public String getName() {
        return "PubMedIngestor";
    }

    @Override
    public int getNumRecords() {
        return -1;
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
        return xmlFileIterator.hasNext();
    }

    private boolean canContinue(int curCount) {
        if (!sampleMode) {
            return true;
        }
        return curCount < sampleSize;
    }

    @Override
    public void beforeShutdown(IDocUpdater docUpdater) {
        // handle deleted pmids
        Map<String,String> payload = new HashMap<String, String>(7);
        for(String pmid : diMap.keySet()) {
            DeleteInfo di = diMap.get(pmid);
            payload.put("primaryKey", pmid);
            payload.put("filename", di.getFileName());
            docUpdater.update(payload);
        }
    }
}
