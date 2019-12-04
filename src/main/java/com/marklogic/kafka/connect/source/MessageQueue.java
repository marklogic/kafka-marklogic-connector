package com.marklogic.kafka.connect.source;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MessageQueue {

    private static MessageQueue ourInstance = new MessageQueue();

    public static MessageQueue getInstance() {
        return ourInstance;
    }

    public static class Message {
        private String topic;
        private String mimeType;
        private String payload;

        public Message(String topic, String mimeType, String payload) {
            this.topic = topic;
            this.mimeType = mimeType;
            this.payload = payload;
        }

        public String getMimeType() {
            return mimeType;
        }

        public String getPayload() {
            return payload;
        }

        public String getTopic() {
            return topic;
        }
    }

    private List<Message> queue;

    private MessageQueue() {
        queue = Collections.synchronizedList(new ArrayList<>());
    }

    public List<Message> getQueue() {
        return queue;
    }

    public void enqueue(Message msg) {
        queue.add(msg);
    }

    public void enqueue(String topic, String mimeType, String payload) {
        queue.add(new Message(topic, mimeType, payload));
    }

    public void clear() { queue.clear(); }

    public int size() { return queue.size(); }
}
