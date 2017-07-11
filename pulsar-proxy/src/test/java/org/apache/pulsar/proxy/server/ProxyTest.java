/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pulsar.proxy.server;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.mockito.Mockito.doReturn;

import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.test.PortManager;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.yahoo.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import com.yahoo.pulsar.client.api.Consumer;
import com.yahoo.pulsar.client.api.Message;
import com.yahoo.pulsar.client.api.Producer;
import com.yahoo.pulsar.client.api.ProducerConfiguration;
import com.yahoo.pulsar.client.api.ProducerConfiguration.MessageRoutingMode;
import com.yahoo.pulsar.client.api.PulsarClient;

public class ProxyTest extends MockedPulsarServiceBaseTest {

    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();

        proxyConfig.setServicePort(PortManager.nextFreePort());
        proxyService = Mockito.spy(new ProxyService(proxyConfig));
        doReturn(mockZooKeeperClientFactory).when(proxyService).getZooKeeperClientFactory();

        proxyService.start();
    }

    @Override
    @AfterClass
    protected void cleanup() throws Exception {
        internalCleanup();

        proxyService.close();
    }

    @Test
    public void testProducer() throws Exception {
        PulsarClient client = PulsarClient.create("pulsar://localhost:" + proxyConfig.getServicePort());
        Producer producer = client.createProducer("persistent://sample/test/local/producer-topic");

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }

        client.close();
    }

    @Test
    public void testProducerConsumer() throws Exception {
        PulsarClient client = PulsarClient.create("pulsar://localhost:" + proxyConfig.getServicePort());
        Producer producer = client.createProducer("persistent://sample/test/local/producer-consumer-topic");

        // Create a consumer directly attached to broker
        Consumer consumer = pulsarClient.subscribe("persistent://sample/test/local/producer-consumer-topic", "my-sub");

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }

        for (int i = 0; i < 10; i++) {
            Message msg = consumer.receive(1, TimeUnit.SECONDS);
            checkNotNull(msg);
            consumer.acknowledge(msg);
        }

        Message msg = consumer.receive(0, TimeUnit.SECONDS);
        checkArgument(msg == null);

        consumer.close();
        client.close();
    }

    @Test
    public void testPartitions() throws Exception {
        PulsarClient client = PulsarClient.create("pulsar://localhost:" + proxyConfig.getServicePort());
        admin.persistentTopics().createPartitionedTopic("persistent://sample/test/local/partitioned-topic", 2);

        ProducerConfiguration producerConf = new ProducerConfiguration();
        producerConf.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        Producer producer = client.createProducer("persistent://sample/test/local/partitioned-topic", producerConf);

        // Create a consumer directly attached to broker
        Consumer consumer = pulsarClient.subscribe("persistent://sample/test/local/partitioned-topic", "my-sub");

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }

        for (int i = 0; i < 10; i++) {
            Message msg = consumer.receive(1, TimeUnit.SECONDS);
            checkNotNull(msg);
        }

        client.close();
    }

}
