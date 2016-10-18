package org.neuinfo.foundry.consumers.common;

import org.apache.commons.net.ftp.FTPFile;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by bozyurt on 10/10/16.
 */
public class FTPUtils {


    public static void  recurseRemoteDirs(String parentDir, FtpClient client, Pattern pattern, List<FileInfo> filteredFiles,
                           boolean sampleMode, int sampleSize, boolean testMode, int maxNumDocs2Ingest) {
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
                    recurseRemoteDirs(fullPath, client, pattern, filteredFiles, sampleMode,sampleSize, testMode, maxNumDocs2Ingest);
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
}
