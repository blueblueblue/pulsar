package org.apache.pulsar.kafkaprototype;

import java.util.Properties;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestBasic {
    private static final Logger log = LoggerFactory.getLogger(TestBasic.class);

    @Test
    public void testFoo() throws Exception {
        DummyServer server = new DummyServer();
        server.startAsync().awaitRunning();
        log.info("Server started at port = {}", server.getPort());

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:" + server.getPort());
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        Producer<String, String> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<String, String>("my-topic", "key1", "value1"));
        producer.flush();

        server.stopAsync();
    }
}
