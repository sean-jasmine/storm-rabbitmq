package main.java.com.impl;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import clojure.lang.Obj;

import java.io.Serializable;
import java.util.List;

public interface MessageBuilder extends Serializable {
    List<Object> deSerialize(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body);
}
