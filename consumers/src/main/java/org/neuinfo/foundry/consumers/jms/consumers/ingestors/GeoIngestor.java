package org.neuinfo.foundry.consumers.jms.consumers.ingestors;

import org.apache.log4j.Logger;
import org.jdom2.Element;
import org.json.JSONObject;
import org.neuinfo.foundry.common.model.IDataParser;
import org.neuinfo.foundry.common.model.InputDataIterator;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.DataParserFactory;
import org.neuinfo.foundry.common.util.Utils;
import org.neuinfo.foundry.common.util.XML2JSONConverter;
import org.neuinfo.foundry.consumers.common.FTPUtils;
import org.neuinfo.foundry.consumers.common.FTPUtils.FileInfo;
import org.neuinfo.foundry.consumers.common.FtpClient;
import org.neuinfo.foundry.consumers.common.LFTPWrapper;
import org.neuinfo.foundry.consumers.plugin.Ingestor;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.io.File;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by bozyurt on 2/14/17.
 */
public class GeoIngestor implements Ingestor {
    String ftpHost;
    String remotePath;
    String outDir;
    String xmlFileNamePattern;
    Map<String, String> optionMap;
    List<FileInfo> seriesDirList;
    Iterator<FileInfo> seriesDirIterator;
    Element curRootEl;
    int curCount = 0;
    static Logger log = Logger.getLogger(GeoIngestor.class);

    @Override
    public void initialize(Map<String, String> options) throws Exception {
        this.ftpHost = options.get("ftpHost");
        this.remotePath = options.get("remotePath");
        this.outDir = options.get("outDir");
        this.xmlFileNamePattern = options.get("xmlFileNamePattern");
        this.optionMap = options;
    }

    @Override
    public void startup() throws Exception {
        this.seriesDirList = new LinkedList<FileInfo>();
        List<FileInfo> level1 = getFilesMatching(this.remotePath, null);
        for (FileInfo l1File : level1) {
            if (FTPUtils.looksLikeADirectory(l1File.getFilePath())) {
                List<FileInfo> level2 = getFilesMatching(l1File.getFilePath(), null);
                for (FileInfo l2File : level2) {
                    if (FTPUtils.looksLikeADirectory(l1File.getFilePath())) {
                        this.seriesDirList.add(l2File);
                    }
                }
            }
        }
        log.info("seriesDirList:" + this.seriesDirList.size());
        this.seriesDirIterator = this.seriesDirList.iterator();
    }

    Element getNextRecord() throws Exception {
        while (this.seriesDirIterator.hasNext()) {
            FileInfo seriesDir = this.seriesDirIterator.next();
            log.info("handling " + seriesDir.getFilePath());
            List<FileInfo> minimlDirs = getFilesMatching(seriesDir.getFilePath(), "miniml$");
            if (!minimlDirs.isEmpty()) {
                FileInfo minimlDir = minimlDirs.get(0);
                List<FileInfo> tarFiles = getFilesMatching(minimlDir.getFilePath(), ".+_family\\.xml\\.tgz$");
                if (!tarFiles.isEmpty()) {
                    FileInfo fi = tarFiles.get(0);
                    FtpClient client = new FtpClient(ftpHost);
                    String remoteFilePath = fi.getFilePath();
                    Assertion.assertTrue(remoteFilePath.startsWith(remotePath));
                    String relativePath = remoteFilePath.substring(remotePath.length());
                    File localFile = new File(outDir, relativePath);
                    localFile.getParentFile().mkdirs();
                    client.transferFile(localFile.getAbsolutePath(), remoteFilePath, true);
                    Assertion.assertNotNull(xmlFileNamePattern);
                    Element rootEl = Utils.extractXMLFromTarWithCleanup(localFile.getAbsolutePath(), xmlFileNamePattern);
                    if (rootEl == null) {
                        continue;
                    }
                    return rootEl;
                }
            }
        }
        return null;
    }

    @Override
    public Result prepPayload() {
        try {
            if (this.curRootEl != null) {
                XML2JSONConverter converter = new XML2JSONConverter();
                JSONObject json = converter.toJSON(this.curRootEl);
                this.curCount++;
                Result r = new Result(json, Result.Status.OK_WITH_CHANGE);
                return r;
            } else {
                return new Result(null, Result.Status.ERROR, "");
            }
        } catch (Throwable t) {
            t.printStackTrace();
            return new Result(null, Result.Status.ERROR, t.getMessage());
        }
    }

    @Override
    public String getName() {
        return "GeoIngestor";
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
        if (this.seriesDirIterator.hasNext()) {
            while (true) {
                try {
                    this.curRootEl = getNextRecord();
                    return curRootEl != null;
                } catch (Exception e) {
                    e.printStackTrace();
                    if (!this.seriesDirIterator.hasNext()) {
                        break;
                    }
                }
            }
        }
        return false;
    }

    public List<FileInfo> getFilesMatching(String remoteDir, String fileNamePattern) throws Exception {
        FtpClient client = new FtpClient(ftpHost);
        Pattern pattern = null;
        if (fileNamePattern != null) {
            pattern = Pattern.compile(fileNamePattern);
        }
        return FTPUtils.getFiles(remoteDir, client, pattern);
    }
}
