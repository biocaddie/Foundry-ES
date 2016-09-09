package org.neuinfo.foundry.consumers.common;

import org.neuinfo.foundry.common.util.Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A wrapper for Unix lftp
 * Created by bozyurt on 4/14/16.
 */
public class LFTPWrapper {
    String sourceURL;
    String includePattern;
    String fileNamePattern;
    String targetDir;
    static final String LFTP = "/usr/bin/lftp";

    public LFTPWrapper(String sourceURL, String fileNamePattern) {
        this.sourceURL = sourceURL;
        this.fileNamePattern = fileNamePattern;
    }

    public List<File> sample(int sampleSize, String outDir) throws Exception {
        List<FileInfo> fiList = list(sourceURL, fileNamePattern, false, true);
        List<FileInfo> files = new ArrayList<FileInfo>(sampleSize);
        int count = 0;
        for (FileInfo fi : fiList) {
            if (!fi.directory) {
                files.add(fi);
                count++;
                if (count >= sampleSize) {
                    break;
                }
            }
        }
        if (!files.isEmpty()) {
            return fetchFiles(sourceURL, files, outDir);
        } else {
            if (!fiList.isEmpty()) {
                FileInfo theFI = fiList.get(0);
                String url = sourceURL + "/" + theFI.name;
                List<FileInfo> filesList = list(url, fileNamePattern, true, false);
                List<FileInfo> filtered = new ArrayList<FileInfo>(sampleSize);
                for (FileInfo fi : filesList) {
                    filtered.add(fi);
                    count++;
                    if (count >= sampleSize) {
                        break;
                    }
                }
                if (!filtered.isEmpty()) {
                    return fetchFiles(url, filtered, outDir);
                }
            }
        }
        return Collections.emptyList();
    }

    public List<File> mirror(String includePattern, String outDir) throws Exception {
        List<String> cmdList = new ArrayList<String>(10);
        cmdList.add(LFTP);
        cmdList.add("-e");
        StringBuilder sb = new StringBuilder(128);
        sb.append("o ").append(sourceURL);
        sb.append(" && mirror --verbose ");
        if (includePattern != null) {
            sb.append("-i ").append(includePattern).append(' ');
        }
        sb.append("-O ").append(outDir).append(' ');
        sb.append("&& quit");
        cmdList.add(sb.toString());
        ProcessBuilder pb = new ProcessBuilder(cmdList);

        System.out.println(pb.command());
        Process process = pb.start();
        String line;
        BufferedReader berr = new BufferedReader(new InputStreamReader(
                process.getErrorStream()));
        while ((line = berr.readLine()) != null) {
            System.out.println(line);
        }
        BufferedReader bin = new BufferedReader(new InputStreamReader(
                process.getInputStream()));
        while ((line = bin.readLine()) != null) {
            System.out.println(line);
        }
        process.waitFor();
        return Utils.findAllFilesMatching(new File(outDir), null);
    }

    List<File> fetchFiles(List<FileInfo> fiList, String outDir) throws Exception {
        return fetchFiles(this.sourceURL, fiList, outDir);
    }

    List<File> fetchFiles(String url, List<FileInfo> fiList, String outDir) throws Exception {
        List<String> cmdList = new ArrayList<String>(10);
        cmdList.add(LFTP);
        cmdList.add("-e");
        StringBuilder sb = new StringBuilder(128);
        sb.append("o ").append(url);
        sb.append(" && mget ");
        for (FileInfo fi : fiList) {
            sb.append(fi.name).append(' ');
        }
        sb.append(" -O ").append(outDir);
        sb.append(" && quit");
        cmdList.add(sb.toString());
        ProcessBuilder pb = new ProcessBuilder(cmdList);

        System.out.println(pb.command());
        Process process = pb.start();
        String line;
        BufferedReader berr = new BufferedReader(new InputStreamReader(
                process.getErrorStream()));
        while ((line = berr.readLine()) != null) {
            System.out.println(line);
        }
        BufferedReader bin = new BufferedReader(new InputStreamReader(
                process.getInputStream()));
        while ((line = bin.readLine()) != null) {
            System.out.println(line);
        }
        process.waitFor();
        File[] files = new File(outDir).listFiles();
        return Arrays.asList(files);
    }

    List<FileInfo> list(String url, String filePattern, boolean recursive, boolean applyPattern2FilesOnly) throws Exception {
        List<String> cmdList = new ArrayList<String>(10);
        cmdList.add(LFTP);
        cmdList.add("-e");
        StringBuilder sb = new StringBuilder(128);
        sb.append("o ").append(url);
        if (recursive) {
            sb.append(" && find && quit");
        } else {
            sb.append(" && ls && quit");
        }
        cmdList.add(sb.toString());
        ProcessBuilder pb = new ProcessBuilder(cmdList);

        System.out.println(pb.command());
        Process process = pb.start();
        BufferedReader bin = new BufferedReader(new InputStreamReader(
                process.getInputStream()));
        List<FileInfo> fiList = new LinkedList<FileInfo>();
        String line;
        Pattern pattern = null;
        if (filePattern != null) {
            pattern = Pattern.compile(filePattern);
        }
        if (recursive) {
            while ((line = bin.readLine()) != null) {
                line = line.replaceFirst("^\\./", "");
                if (pattern != null) {
                    Matcher m = pattern.matcher(line);
                    if (m.find()) {
                        fiList.add(new FileInfo(line, false));
                    }
                } else {
                    fiList.add(new FileInfo(line, false));
                }
            }
        } else {
            while ((line = bin.readLine()) != null) {
                System.out.println(line);
                String[] toks = line.split("\\s+");
                boolean directory = toks[0].startsWith("d");
                String filename = toks[toks.length - 1];
                if (pattern != null) {
                    if (applyPattern2FilesOnly && directory) {
                        fiList.add(new FileInfo(filename, directory));
                    } else {
                        Matcher m = pattern.matcher(filename);
                        if (m.find()) {
                            fiList.add(new FileInfo(filename, directory));
                        }
                    }
                } else {
                    fiList.add(new FileInfo(filename, directory));
                }
            }
        }
        process.waitFor();
        return fiList;
    }


    public static class FileInfo {
        String name;
        boolean directory = false;

        public FileInfo(String name, boolean directory) {
            this.name = name;
            this.directory = directory;
        }
    }

    public static void main(String[] args) throws Exception {
        LFTPWrapper lftp = new LFTPWrapper("ftp.ncbi.nlm.nih.gov/dbgap/studies", "data_dict\\.xml$");

        lftp.sample(2, "/tmp/sample");
    }
}
