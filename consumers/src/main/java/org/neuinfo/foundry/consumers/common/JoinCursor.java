package org.neuinfo.foundry.consumers.common;

import org.json.JSONObject;
import org.neuinfo.foundry.common.ingestion.IngestCommandInfo;
import org.neuinfo.foundry.common.model.ColumnMeta;
import org.neuinfo.foundry.common.util.Assertion;
import org.neuinfo.foundry.consumers.common.DoublyLinkedList.Node;
import org.neuinfo.foundry.consumers.common.WebJoinIterator.WebJoinInfo;
import org.neuinfo.foundry.consumers.plugin.IngestorIterable;
import org.neuinfo.foundry.consumers.plugin.Result;

import java.util.*;

/**
 * Created by bozyurt on 2/27/17.
 */
public class JoinCursor implements IngestorIterable {
    List<IngestCommandInfo> joinCommands;
    Map<String, Joinable> cursorMap;
    List<CursorMeta> cmList;
    Map<String, CursorMeta> cursorMetaMap = new HashMap<String, CursorMeta>(11);
    Map<String, String> joinRightPartMap = new HashMap<String, String>();
    DoublyLinkedList<Joinable> joinState = new DoublyLinkedList<Joinable>();
    Result currentRecord;

    public JoinCursor(List<IngestCommandInfo> joinCommands, LinkedHashMap<String, Joinable> cursorMap) {
        this.joinCommands = joinCommands;
        this.cursorMap = cursorMap;
        this.cmList = new ArrayList<CursorMeta>(cursorMap.size());
        for (String alias : cursorMap.keySet()) {
            Joinable cursor = cursorMap.get(alias);
            CursorMeta cm = new CursorMeta(alias, cursor);
            cursorMetaMap.put(cm.getAlias(), cm);
            cmList.add(cm);
            joinState.add(cursor);
        }

    }

    @Override
    public void initialize(Map<String, String> options) throws Exception {
    }

    @Override
    public void startup() throws Exception {
        for (IngestCommandInfo joinCmd : joinCommands) {
            WebJoinInfo wji = WebJoinInfo.parse(joinCmd.getJoinStr());
            CursorMeta primaryCM = cursorMetaMap.get(wji.getPrimaryAlias());
            Assertion.assertNotNull(primaryCM);
            primaryCM.setJoinInfo(wji);
            primaryCM.setFieldJsonPath(wji.getPrimaryJsonPath());
            CursorMeta secCM = cursorMetaMap.get(wji.getSecondaryAlias());
            Assertion.assertNotNull(secCM);
            joinRightPartMap.put(wji.getSecondaryAlias(), wji.getSecondaryJsonPath());
            secCM.setRecordJsonPath(wji.getSecondaryRecordJsonPath());
            secCM.setFieldJsonPath(wji.secondaryJsonPath);
        }
        boolean first = true;
        for (CursorMeta cm : this.cmList) {
            Joinable cursor = cursorMap.get(cm.getAlias());
            if (first) {
                first = false;
                cursor.setResetJsonPath(null);
                cursor.setJoinValueJsonPath(cm.getFieldJsonPath());
            } else {
                String joinRightPartJsonPath = this.joinRightPartMap.get(cursor.getAlias());
                cursor.setResetJsonPath(joinRightPartJsonPath);
                cursor.setJoinValueJsonPath(cm.getFieldJsonPath());
            }
        }
    }

    @Override
    public Result prepPayload() {
        return this.currentRecord;
    }

    @Override
    public boolean hasNext() {
        Node<Joinable> tail = joinState.getTail();
        Node<Joinable> head = joinState.getHead();
        try {
            if (tail.getPayload().peek() == null) {
                // initialize
                Node<Joinable> n = head;
                String joinValue = null;
                while (n != null) {
                    Joinable payload = n.getPayload();
                    payload.reset(joinValue);
                    if (payload.hasNext()) {
                        payload.next();
                        joinValue = payload.getJoinValue();
                    } else {
                        if (n != tail) {
                            n = n.getPrev();
                            if (n == head) {
                                joinValue = null;
                            }
                            continue;
                        } else {
                            return false;
                        }
                    }
                    n = n.getNext();
                }
                buildJoinRecord();
                return true;
            } else {
                Joinable payload = tail.getPayload();
                if (payload.hasNext()) {
                    payload.next();
                    buildJoinRecord();
                    return true;
                } else {
                    Node<Joinable> n = tail.getPrev();
                    Node<Joinable> hasNextNode = null;
                    while (n != null) {
                        if (n.getPayload().hasNext()) {
                            hasNextNode = n;
                            break;
                        }
                        n = n.getPrev();
                    }
                    if (hasNextNode == null) {
                        // nothing left to join
                        return false;
                    }
                    n = hasNextNode;
                    String joinValue = null;
                    while (n != null) {
                        payload = n.getPayload();
                        payload.reset(joinValue);
                        if (payload.hasNext()) {
                            payload.next();
                            joinValue = payload.getJoinValue();
                        } else {
                            if (n != null) {
                                throw new RuntimeException("Should not happen!");
                            }
                        }
                        n = n.getNext();
                    }
                    buildJoinRecord();
                    return true;
                }
            }
        } catch (Exception x) {
            x.printStackTrace();
        }

        return false;
    }

    private void buildJoinRecord() {
        Node<Joinable> tail = joinState.getTail();
        JSONObject json = new JSONObject(tail.getPayload().peek().toString());
        Node<Joinable> p = tail.getPrev();
        while (p != null) {
            Joinable payload = p.getPayload();
            //CursorMeta cm = cursorMetaMap.get(payload.getAlias());
            String fieldName = payload.getAlias();
            JSONObject js = new JSONObject(payload.peek().toString());
            List<ColumnMeta> columnMetaList = payload.getColumnMetaList();
            JSONObject copy = new JSONObject(js.toString());
            copy = CursorUtils.handleExternalColumnNames(copy, columnMetaList);
            json.put(fieldName, copy);
            p = p.getPrev();
        }
        this.currentRecord = new Result(json, Result.Status.OK_WITH_CHANGE);
    }
}
