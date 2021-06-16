package com.kuze.bigdata.study.domain;

public class KafkaMessage {

    private byte[] bytes;

    public KafkaMessage(byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] getBytes() {
        return bytes;
    }


}
