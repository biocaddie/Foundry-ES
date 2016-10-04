package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import org.apache.commons.net.ftp.FTPFile;
import org.apache.log4j.Logger;
import org.jdom2.Element;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.consumers.common.*;
import org.neuinfo.foundry.consumers.plugin.Ingestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bozyurt on 3/26/15.
 */
public class FTPIngestor implements Ingestor {
    String ftpHost;
    String remotePath;
    String pathPattern;
    String pattern1Type;
    String pattern2Type;
    String outDir;
    String fileNamePattern;
    String docElName;
    String topElName;
    boolean recursive = false;
    Map<String, String> optionMap;
    // Iterator<File> fileIterator;
    int numOfRecords = -1;
    File extractionDir;
    boolean testMode = false;
    int maxNumDocs2Ingest = -1;
    int count = 0;
    boolean sampleMode = false;
    int sampleSize = 1;
    XMLFileIterator xmlFileIterator;
    final static int TEST_MAX_FILES = 10;
    static Logger log = Logger.getLogger(FTPIngestor.class);

    @Override
    public void initialize(Map<String, String> options) throws Exception {
        this.ftpHost = options.get("ftpHost");
        this.remotePath = options.get("remotePath");
        this.outDir = options.get("outDir");
        this.fileNamePattern = options.get("filenamePattern");

        this.docElName = options.get("documentElement");
        this.topElName = options.containsKey("topElement") ? options.get("topElement") : null;
        this.pathPattern = options.get("pathPattern");
        this.pattern1Type = options.get("pattern1Type");
        this.pattern2Type = options.get("pattern2Type");
        this.recursive = options.containsKey("recursive") ? Boolean.parseBoolean(options.get("recursive")) : false;
        this.optionMap = options;
        if (options.containsKey("maxDocs")) {
            this.maxNumDocs2Ingest = Utils.getIntValue(options.get("maxDocs"), -1);
        }
        if (options.containsKey("testMode")) {
            this.testMode = Boolean.parseBoolean(options.get("testMode"));
        }
        this.sampleMode = options.containsKey("sampleMode")
                ? Boolean.parseBoolean(options.get("sampleMode")) : false;

        this.sampleSize = Utils.getIntValue(options.get("sampleSize"), 1);
    }


    @Override
    public void startup() throws Exception {
        if (recursive && fileNamePattern != null) {


            List<FileInfo> filesMatching = getFilesMatching(this.fileNamePattern, recursive);

            FtpClient client = new FtpClient(ftpHost);
            List<File> localFiles = new ArrayList<File>(filesMatching.size());
            int i = 0;
            for (FileInfo fi : filesMatching) {
                String remoteFilePath = fi.getFilePath();
                Assertion.assertTrue(remoteFilePath.startsWith(remotePath));
                String relativePath = remoteFilePath.substring(remotePath.length());
                File localFile = new File(outDir, relativePath);
                localFile.getParentFile().mkdirs();
                // check if file is already cached and same size as the remote file. If so use the local file
                if (!localFile.isFile() || localFile.length() != fi.getSize()) {
                    client.transferFile(localFile.getAbsolutePath(), remoteFilePath, true);
                }

                /*
                if (localFile.getName().endsWith(".gz")) {
                    Gunzipper gunzipper = new Gunzipper(localFile);
                    gunzipper.execute();
                    String filePath = localFile.getAbsolutePath().replaceFirst("\\.gz$", "");
                    File localExpandedFile = new File(filePath);
                    if (localExpandedFile.isFile()) {
                        localFiles.add(localExpandedFile);
                        i++;
                        if (localFile.isFile()) {
                            localFile.delete();
                        }
                        if (!canContinue(i)) {
                            break;
                        }
                    }
                } else {
                }
                */
                localFiles.add(localFile);
                i++;
                if (!canContinue(i)) {
                    break;
                }

                log.info(remoteFilePath);
            }
            this.xmlFileIterator = new XMLFileIterator(
                    new RemoteFileIterator(localFiles),
                    this.topElName, this.docElName);
            return;
        }

        if (remotePath == null) {
            Assertion.assertNotNull(this.pathPattern);
            Assertion.assertNotNull(this.pattern1Type);
            Assertion.assertNotNull(this.pattern2Type);
            String latestFilePath = determineMostRecentFile2Download();
            log.info("found latestFilePath:" + latestFilePath);
            this.remotePath = latestFilePath;
        }

        int idx = remotePath.lastIndexOf('/');
        String basename = remotePath.substring(idx + 1);
        File localFile = new File(outDir, basename);
        if (!localFile.isFile()) {
            FtpClient client = new FtpClient(ftpHost);
            client.transferFile(localFile.getAbsolutePath(), remotePath, true);
        }
        if (basename.endsWith(".xml")) {
            this.xmlFileIterator = new XMLFileIterator(
                    new RemoteFileIterator(Arrays.asList(localFile)),
                    this.topElName, this.docElName);
            return;
        }
        String extractionDirname = ftpHost.replaceAll("[\\./]+", "_");
        this.extractionDir = new File(outDir, extractionDirname);
        if (!extractionDir.isDirectory()) {
            extractionDir.mkdir();
        }
        log.info("unpacking " + localFile + " to " + extractionDir);
        Unpacker unpacker = new Unpacker(localFile, extractionDir);
        unpacker.unpack();
        Pattern pattern = Pattern.compile(fileNamePattern);
        List<File> files = findFiles(extractionDir, pattern);
        if (sampleMode) {
            files = files.subList(0, sampleSize);
        }
        this.xmlFileIterator = new XMLFileIterator(
                new RemoteFileIterator(files), this.topElName, this.docElName);

    }


    @Override
    public Result prepPayload() {
        try {
            Result r = ConsumerUtils.convert2JSON(this.xmlFileIterator.next());
            count++;
            return r;
        } catch (Throwable t) {
            return ConsumerUtils.toErrorResult(t, log);
        }
    }


    @Override
    public String getName() {
        return "FTPIngestor";
    }

    @Override
    public int getNumRecords() {
        return this.numOfRecords;
    }

    @Override
    public String getOption(String optionName) {
        return optionMap.get(optionName);
    }

    @Override
    public void shutdown() {
        if (this.extractionDir != null && this.extractionDir.isDirectory()) {
            Utils.deleteRecursively(this.extractionDir);
        }
    }

    @Override
    public boolean hasNext() {
        if (this.maxNumDocs2Ingest > 0 && this.count >= this.maxNumDocs2Ingest) {
            return false;
        }
        return this.xmlFileIterator.hasNext();
    }


    public static class FileInfo {
        final String filePath;
        final long size;

        public FileInfo(String filePath, long size) {
            this.filePath = filePath;
            this.size = size;
        }

        public String getFilePath() {
            return filePath;
        }

        public long getSize() {
            return size;
        }
    }

    public List<FileInfo> getFilesMatching(String fnamePattern, boolean recurseDirs) throws Exception {
        String remoteDir = this.remotePath;
        FtpClient client = new FtpClient(ftpHost);
        Pattern pattern = Pattern.compile(fnamePattern);
        List<FileInfo> filteredFiles = new LinkedList<FileInfo>();
        if (recurseDirs) {
            recurseRemoteDirs(remoteDir, client, pattern, filteredFiles);
        } else {
            //TODO
            // List<String> list = client.list(remoteDir);
            throw new RuntimeException("Non recursive option is not supported yet!");
        }
        return filteredFiles;
    }

    void recurseLocalDirs(File parentDir, String extension, List<FileInfo> filteredFiles) {
        if (testMode && filteredFiles.size() >= maxNumDocs2Ingest) {
            return;
        }
        if (sampleMode && filteredFiles.size() >= sampleSize) {
            return;
        }
        File[] files = parentDir.listFiles();
        for (File f : files) {
            if (f.isFile() && f.getName().endsWith(extension)) {
                if (testMode && filteredFiles.size() >= maxNumDocs2Ingest) {
                    return;
                }
                filteredFiles.add(new FileInfo(f.getAbsolutePath(), f.length()));
            } else if (f.isDirectory()) {
                recurseLocalDirs(f, extension, filteredFiles);
            }
        }
    }

    void recurseRemoteDirs(String parentDir, FtpClient client, Pattern pattern, List<FileInfo> filteredFiles) {
        if (testMode && filteredFiles.size() >= maxNumDocs2Ingest) {
            return;
        }
        if (sampleMode && filteredFiles.size() >= sampleSize) {
            return;
        }
        List<FTPFile> list = client.list(parentDir);
        if (list.isEmpty()) {
            return;
        }
        for (FTPFile path : list) {
            if (testMode && filteredFiles.size() >= maxNumDocs2Ingest) {
                return;
            }
            if (sampleMode && filteredFiles.size() >= sampleSize) {
                return;
            }
            Matcher m = pattern.matcher(path.getName());
            if (m.find()) {
                String fullPath = toFullPath(path.getName(), parentDir);
                filteredFiles.add(new FileInfo(fullPath, path.getSize()));

            } else {
                // anything having no file extension is assumed to be a directory
                if (path.isDirectory()) {
                    String fullPath = toFullPath(path.getName(), parentDir);
                    recurseRemoteDirs(fullPath, client, pattern, filteredFiles);
                }
            }
        }
    }

    public static String toFullPath(String path, String parentDir) {
        if (!path.startsWith(parentDir)) {
            if (parentDir.endsWith("/")) {
                if (path.startsWith("/")) {
                    return parentDir + path.substring(1);
                } else {
                    return parentDir + path;
                }
            } else {
                if (path.startsWith("/")) {
                    return parentDir + path;
                } else {
                    return parentDir + "/" + path;
                }
            }
        } else {
            return path;
        }

    }

    private boolean canContinue(int curCount) {
        if (!sampleMode) {
            return true;
        }
        return curCount < sampleSize;
    }


    List<File> findFiles(File rootDir, Pattern filePattern) {
        List<File> filteredFiles = new LinkedList<File>();
        findFiles(rootDir, filePattern, filteredFiles);
        return filteredFiles;
    }

    void findFiles(File parentDir, Pattern filePattern, List<File> filteredFiles) {
        File[] files = parentDir.listFiles();
        for (File f : files) {
            if (f.isDirectory()) {
                findFiles(f, filePattern, filteredFiles);
            } else {
                Matcher matcher = filePattern.matcher(f.getName());
                if (matcher.find()) {
                    filteredFiles.add(f);
                }
            }
        }
    }

    public String determineMostRecentFile2Download() throws Exception {
        FtpClient client = new FtpClient(ftpHost);
        int startIdx = pathPattern.indexOf("%[");
        int endIdx = pathPattern.indexOf("]%");
        Assertion.assertTrue(startIdx != -1);
        String regexStr = pathPattern.substring(startIdx + 2, endIdx);
        String pathPrefix = pathPattern.substring(0, startIdx);
        if (pathPrefix.endsWith("/")) {
            pathPrefix = pathPrefix.substring(0, pathPrefix.length() - 1);
        }
        Pattern p1 = Pattern.compile("(" + regexStr + ")");
        List<FTPFile> list;

/*
        if (testMode) {
            // For test
            list = new ArrayList<String>();
            list.add("/pub/databases/biomodels/weekly_archives/2013");
            list.add("/pub/databases/biomodels/weekly_archives/2014");
            list.add("/pub/databases/biomodels/weekly_archives/2015");
        } else {
*/
        list = client.list(pathPrefix);

        Assertion.assertNotNull(pattern1Type);
        SimpleDateFormat sdf = prepDateFormat(pattern1Type);
        Date maxDate = null;
        String latestPath = null;
        for (FTPFile aPath : list) {
            Matcher m = p1.matcher(aPath.getName());
            if (m.find()) {
                String dateStr = m.group(1);
                Date d = sdf.parse(dateStr);
                if (maxDate == null || maxDate.before(d)) {
                    maxDate = d;
                    latestPath = pathPrefix + "/" + aPath.getName();
                }
            }
        }
        log.info("latestPath:" + latestPath);
        startIdx = pathPattern.indexOf("%[", endIdx + 2);
        if (startIdx == -1) {
            return latestPath;
        }
        int oldEndIdx = endIdx;
        endIdx = pathPattern.indexOf("]%", startIdx + 2);
        regexStr = pathPattern.substring(startIdx + 2, endIdx);
        String pathSection = pathPattern.substring(oldEndIdx + 2, startIdx);
        int lastPathSepIdx = pathSection.indexOf('/', 1);
        if (lastPathSepIdx != -1) {
            latestPath += pathSection.substring(0, lastPathSepIdx);
        }
        List<FTPFile> filesList = null;
        /*
        if (testMode) {
            // FIXME test
            filesList = new ArrayList<String>(5);
            filesList.add("/pub/databases/biomodels/weekly_archives/2015/BioModels-Database-weekly-2015-01-23-sbmls.tar.bz2");
            filesList.add("/pub/databases/biomodels/weekly_archives/2015/BioModels-Database-weekly-2015-02-23-sbmls.tar.bz2");
            filesList.add("/pub/databases/biomodels/weekly_archives/2015/BioModels-Database-weekly-2015-02-13-sbmls.tar.bz2");
            filesList.add("/pub/databases/biomodels/weekly_archives/2015/BioModels-Database-weekly-2015-03-23-sbmls.tar.bz2");
        } else {
        */
        filesList = client.list(latestPath);
        // }
        Assertion.assertNotNull(pattern2Type);
        sdf = prepDateFormat(pattern2Type);
        Pattern p2 = Pattern.compile("(" + regexStr + ")");
        maxDate = null;
        String latestFilePath = null;
        for (FTPFile aPath : filesList) {
            Matcher m = p2.matcher(aPath.getName());
            if (m.find()) {
                String dateStr = m.group(1);
                Date d = sdf.parse(dateStr);
                if (maxDate == null || maxDate.before(d)) {
                    maxDate = d;
                    latestFilePath = latestPath + "/" + aPath.getName();
                }
            }
        }
        return latestFilePath;
    }

    public boolean isTestMode() {
        return testMode;
    }

    public void setTestMode(boolean testMode) {
        this.testMode = testMode;
    }

    static SimpleDateFormat prepDateFormat(String patternType) {
        int idx = patternType.indexOf('_');
        Assertion.assertTrue(idx != -1);
        return new SimpleDateFormat(patternType.substring(idx + 1));
    }
}
