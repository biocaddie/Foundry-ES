package org.neuinfo.foundry.common.ingestion;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by bozyurt on 2/22/17.
 */
public class IngestCommandInfo {
    String commandName;
    String alias;
    String path;
    FormatType formatType;
    FileType fileType;
    String fromAlias;
    String topElement;
    String docElement;
    boolean linePerRecord = false;
    IngestCommandInfo left;
    IngestCommandInfo right;
    String joinStr;
    private String user;
    private String password;
    public final static String DOWNLOAD = "download";
    public final static String EXTRACT = "extract";
    public final static String JOIN = "join";
    public final static String INGEST = "ingest";
    Map<String,String> paramMap = new HashMap<String, String>(17);

    public IngestCommandInfo(String commandName) {
        this.commandName = commandName;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getFromAlias() {
        return fromAlias;
    }

    public void setFromAlias(String fromAlias) {
        this.fromAlias = fromAlias;
    }

    public String getCommandName() {
        return commandName;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public FormatType getFormatType() {
        return formatType;
    }

    public void setFormatType(FormatType formatType) {
        this.formatType = formatType;
    }

    public FileType getFileType() {
        return fileType;
    }

    public void setFileType(FileType fileType) {
        this.fileType = fileType;
    }

    public String getTopElement() {
        return topElement;
    }

    public void setTopElement(String topElement) {
        this.topElement = topElement;
    }

    public String getDocElement() {
        return docElement;
    }

    public void setDocElement(String docElement) {
        this.docElement = docElement;
    }

    public boolean isLinePerRecord() {
        return linePerRecord;
    }

    public void setLinePerRecord(boolean linePerRecord) {
        this.linePerRecord = linePerRecord;
    }

    public Map<String, String> getParamMap() {
        return paramMap;
    }

    public IngestCommandInfo getLeft() {
        return left;
    }

    public void setLeft(IngestCommandInfo left) {
        this.left = left;
    }

    public IngestCommandInfo getRight() {
        return right;
    }

    public void setRight(IngestCommandInfo right) {
        this.right = right;
    }

    public String getJoinStr() {
        return joinStr;
    }

    public void setJoinStr(String joinStr) {
        this.joinStr = joinStr;
    }
}
