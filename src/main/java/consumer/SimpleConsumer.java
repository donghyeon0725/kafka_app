package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * 심플 컨슈머 애플리케이션 예제
 *
 * 토픽 생성 명령어
 * bin/kafka-console-producer.sh --bootstrap-server <host>:9092 --topic <topicName>
 * */
public class SimpleConsumer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    // 토픽명
    private final static String TOPIC_NAME = "hello.kafka";
    // 컨슈머 그룹명 => 구독형태로 사용하면 꼭 필요
    private final static String GROUP_ID = "hello-group";
    // 카프카 클러스터 주소
    private final static String BOOTSTRAP_SERVERS = "3.34.130.109:9092";

    public static void main(String[] args) {
        // 프로퍼티 생성
        Properties configs = new Properties();

        // 필수 값
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        // 컨슈머가 토픽 구독
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        while (true) {
            // 반복 가능한 객체로 되어 있음
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record : records) {
                // ConsumerRecord(topic = hello.kafka, partition = 2, leaderEpoch = 0, offset = 2, CreateTime = 1643813098898, serialized key size = -1, serialized value size = 11, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = testMessage)
                logger.info("record: {}", record);
            }
        }
    }

/**
 * 로그
 * */
/*
[main] INFO org.apache.kafka.clients.consumer.ConsumerConfig - ConsumerConfig values:
	allow.auto.create.topics = true
	auto.commit.interval.ms = 5000
	auto.offset.reset = latest
	bootstrap.servers = [3.34.130.109:9092]
	check.crcs = true
	client.dns.lookup = default
	client.id =
	client.rack =
	connections.max.idle.ms = 540000
	default.api.timeout.ms = 60000
	enable.auto.commit = true
	exclude.internal.topics = true
	fetch.max.bytes = 52428800
	fetch.max.wait.ms = 500
	fetch.min.bytes = 1
	group.id = hello-group
	group.instance.id = null
	heartbeat.interval.ms = 3000
	interceptor.classes = []
	internal.leave.group.on.close = true
	isolation.level = read_uncommitted
	key.deserializer = class org.apache.kafka.common.serialization.StringDeserializer
	max.partition.fetch.bytes = 1048576
	max.poll.interval.ms = 300000
	max.poll.records = 500
	metadata.max.age.ms = 300000
	metric.reporters = []
	metrics.num.samples = 2
	metrics.recording.level = INFO
	metrics.sample.window.ms = 30000
	partition.assignment.strategy = [class org.apache.kafka.clients.consumer.RangeAssignor]
	receive.buffer.bytes = 65536
	reconnect.backoff.max.ms = 1000
	reconnect.backoff.ms = 50
	request.timeout.ms = 30000
	retry.backoff.ms = 100
	sasl.client.callback.handler.class = null
	sasl.jaas.config = null
	sasl.kerberos.kinit.cmd = /usr/bin/kinit
	sasl.kerberos.min.time.before.relogin = 60000
	sasl.kerberos.service.name = null
	sasl.kerberos.ticket.renew.jitter = 0.05
	sasl.kerberos.ticket.renew.window.factor = 0.8
	sasl.login.callback.handler.class = null
	sasl.login.class = null
	sasl.login.refresh.buffer.seconds = 300
	sasl.login.refresh.min.period.seconds = 60
	sasl.login.refresh.window.factor = 0.8
	sasl.login.refresh.window.jitter = 0.05
	sasl.mechanism = GSSAPI
	security.protocol = PLAINTEXT
	security.providers = null
	send.buffer.bytes = 131072
	session.timeout.ms = 10000
	ssl.cipher.suites = null
	ssl.enabled.protocols = [TLSv1.2]
	ssl.endpoint.identification.algorithm = https
	ssl.key.password = null
	ssl.keymanager.algorithm = SunX509
	ssl.keystore.location = null
	ssl.keystore.password = null
	ssl.keystore.type = JKS
	ssl.protocol = TLSv1.2
	ssl.provider = null
	ssl.secure.random.implementation = null
	ssl.trustmanager.algorithm = PKIX
	ssl.truststore.location = null
	ssl.truststore.password = null
	ssl.truststore.type = JKS
	value.deserializer = class org.apache.kafka.common.serialization.StringDeserializer

[main] INFO org.apache.kafka.common.utils.AppInfoParser - Kafka version: 2.5.0
[main] INFO org.apache.kafka.common.utils.AppInfoParser - Kafka commitId: 66563e712b0b9f84
[main] INFO org.apache.kafka.common.utils.AppInfoParser - Kafka startTimeMs: 1643812911555
[main] INFO org.apache.kafka.clients.consumer.KafkaConsumer - [Consumer clientId=consumer-hello-group-1, groupId=hello-group] Subscribed to topic(s): hello.kafka
[main] INFO org.apache.kafka.clients.Metadata - [Consumer clientId=consumer-hello-group-1, groupId=hello-group] Cluster ID: lnj0AvC6RwWfwnqu6M51xg
[main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator - [Consumer clientId=consumer-hello-group-1, groupId=hello-group] Discovered group coordinator 3.34.130.109:9092 (id: 2147483647 rack: null)
[main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator - [Consumer clientId=consumer-hello-group-1, groupId=hello-group] (Re-)joining group
[main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator - [Consumer clientId=consumer-hello-group-1, groupId=hello-group] Join group failed with org.apache.kafka.common.errors.MemberIdRequiredException: The group member needs to have a valid member id before actually entering a consumer group
[main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator - [Consumer clientId=consumer-hello-group-1, groupId=hello-group] (Re-)joining group
[main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-hello-group-1, groupId=hello-group] Finished assignment for group at generation 1: {consumer-hello-group-1-3eb0d694-5240-46e9-8e32-4ed57569138d=Assignment(partitions=[hello.kafka-0, hello.kafka-1, hello.kafka-2])}
[main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator - [Consumer clientId=consumer-hello-group-1, groupId=hello-group] Successfully joined group with generation 1
[main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-hello-group-1, groupId=hello-group] Adding newly assigned partitions: hello.kafka-2, hello.kafka-1, hello.kafka-0
[main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-hello-group-1, groupId=hello-group] Found no committed offset for partition hello.kafka-2
[main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-hello-group-1, groupId=hello-group] Found no committed offset for partition hello.kafka-1
[main] INFO org.apache.kafka.clients.consumer.internals.ConsumerCoordinator - [Consumer clientId=consumer-hello-group-1, groupId=hello-group] Found no committed offset for partition hello.kafka-0
[main] INFO org.apache.kafka.clients.consumer.internals.SubscriptionState - [Consumer clientId=consumer-hello-group-1, groupId=hello-group] Resetting offset for partition hello.kafka-2 to offset 2.
[main] INFO org.apache.kafka.clients.consumer.internals.SubscriptionState - [Consumer clientId=consumer-hello-group-1, groupId=hello-group] Resetting offset for partition hello.kafka-1 to offset 1.
[main] INFO org.apache.kafka.clients.consumer.internals.SubscriptionState - [Consumer clientId=consumer-hello-group-1, groupId=hello-group] Resetting offset for partition hello.kafka-0 to offset 9.
* */
}
