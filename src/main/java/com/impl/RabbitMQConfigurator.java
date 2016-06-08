package main.java.com.impl;

import org.apache.storm.topology.OutputFieldsDeclarer;

import java.io.Serializable;
import java.util.List;

public interface RabbitMQConfigurator extends Serializable {
    String getURL();

    boolean isAutoAcking();

    int getPrefetchCount();

    boolean isReQueueOnFail();

    String getConsumerTag();

    List<String> getQueueName();

    MessageBuilder getMessageBuilder();

    void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer);

    int queueSize();
}
