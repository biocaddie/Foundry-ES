package org.neuinfo.foundry.consumers.common;

/**
 * Created by bozyurt on 2/17/17.
 */
public class DoublyLinkedList<T> {
    Node<T> head;
    Node<T> tail;

    public void add(T payload) {
        if (tail == null) {
            head = new Node<T>(payload, null);
            tail = head;
        } else {
            Node<T> node = new Node<T>(payload, tail);
            tail.next = node;
            tail = node;
        }
    }

    public Node<T> getHead() {
        return head;
    }

    public Node<T> getTail() {
        return tail;
    }

    public static class Node<T> {
        T payload;
        Node next = null;
        Node prev = null;

        public Node(T payload, Node prev) {
            this.payload = payload;
            this.prev = prev;
        }

        public T getPayload() {
            return payload;
        }

        public Node getNext() {
            return next;
        }

        public Node getPrev() {
            return prev;
        }
    }
}
