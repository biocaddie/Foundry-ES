package org.neuinfo.foundry.consumers.common;

import org.apache.log4j.Logger;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.common.util.Utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * <p>Handles downloading and expansion of PMC Open Access full article set. It also creates
 * a persistent map of PMID to full article cache path (<code>pmid.map</code>).
 * </p>
 * Created by bozyurt on 4/19/16.
 */
public class PMCOASetHandler {
    String outDir;
    static final String[] remoteFiles = {"articles.A-B.tar.gz",
            "articles.C-H.tar.gz", "articles.I-N.tar.gz",
            "articles.O-Z.tar.gz"};
    static Logger log = Logger.getLogger(PMCOASetHandler.class);
    static Pattern pmidPattern = Pattern.compile("<article-id\\s+pub-id-type=\"pmid\">(\\d+)<");
    private static ICacheManager<String, File> pmid2PaperPathCacheManager = null;

    public PMCOASetHandler(String outDir) {
        this.outDir = outDir;
        File f = new File(outDir);
        if (!f.isDirectory()) {
            f.mkdirs();
        }
    }

    public synchronized static ICacheManager<String, File> getPmid2PaperPathCacheManager() {
        return pmid2PaperPathCacheManager;
    }

    public void handle() throws Exception {
        LFTPWrapper lftp = new LFTPWrapper("ftp://ftp.ncbi.nlm.nih.gov/pub/pmc", null);
        List<LFTPWrapper.FileInfo> fiList = new ArrayList<LFTPWrapper.FileInfo>(remoteFiles.length);
        for (String remoteFile : remoteFiles) {
            fiList.add(new LFTPWrapper.FileInfo(remoteFile, false));
        }
        File downloadDir = new File(outDir, "downloads");
        File articlesDir = new File(outDir, "articles");
        downloadDir.mkdirs();
        articlesDir.mkdirs();
        Assertion.assertTrue(downloadDir.isDirectory());
        Assertion.assertNotNull(articlesDir.isDirectory());
        List<File> localFiles = lftp.fetchFiles(fiList, downloadDir.getAbsolutePath());

        for (File localFile : localFiles) {
            log.info("unpacking " + localFile + " to " + articlesDir);
            Unpacker unpacker = new Unpacker(localFile, articlesDir);
            unpacker.unpack();
            log.info("unpacked " + localFile + " to " + articlesDir);
        }
        prepPMID2PathCache(true, articlesDir);

    }

    void prepPMID2PathCache(boolean removeExistingDBFile, File articlesDir) {
        File dbFile = new File(outDir, "pmid.map");
        if (removeExistingDBFile && dbFile.isFile()) {
            dbFile.delete();
        }
        synchronized (this) {
            pmid2PaperPathCacheManager = new PMID2ArticlePathCacheManager(dbFile.getAbsolutePath());
        }
        cachePMID2Path(articlesDir);
    }

    void cachePMID2Path(File parentFile) {
        if (parentFile.isFile() && parentFile.getName().endsWith(".nxml")) {
            try {
                String text = Utils.loadAsString(parentFile.getAbsolutePath());
                Matcher matcher = pmidPattern.matcher(text);
                if (matcher.find()) {
                    String pmid = matcher.group(1);
                    pmid2PaperPathCacheManager.put(pmid, parentFile);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            File[] files = parentFile.listFiles();
            for (File f : files) {
                cachePMID2Path(f);
            }
        }
    }

    private static class PMID2ArticlePathCacheManager implements ICacheManager<String, File> {
        DB db;
        ConcurrentNavigableMap<String, File> treeMap;

        private PMID2ArticlePathCacheManager(String dbFile) {
            db = DBMaker.newFileDB(new File(dbFile)).make();
            treeMap = db.getTreeMap("map");
        }

        public File get(String key) {
            File f = treeMap.get(key);
            if (f != null && !f.isFile()) {
                treeMap.remove(key);
                return null;
            }
            return f;
        }

        public void put(String key, File value) {
            treeMap.put(key, value);
            db.commit();
        }

        public void shutdown() {
            if (db != null) {
                db.close();
            }
        }
    }
}
