package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 프로듀셔 애플리케이션 심플 예제
 *
 * 토픽 내부의 레코드 확인
 *
 * 키 없이 확인
 * bin/kafka-console-consumer.sh --bootstrap-server <host>:9092 --topic hello.kafka --from-beginning
 *
 * 키 포함 확인
 * bin/kafka-console-consumer.sh --bootstrap-server <host>:9092 --topic hello.kafka --property print.key=true --property key.separator="-" --from-beginning
 *
 * */
public class SimpleProducer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    // 토픽명
    private final static String TOPIC_NAME = "hello.kafka";
    // 카프카 클러스터 주소
    private final static String BOOTSTRAP_SERVERS = "3.34.130.109:9092";


    public static void main(String[] args) {
        // 프로퍼티 생성
        Properties configs = new Properties();

        // 필수 값
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 프로듀서 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        // 전송

        // 키 없는 레코드
        ProducerRecord<String, String> noKeyRecord = new ProducerRecord<>(TOPIC_NAME, "messageValue");
        producer.send(noKeyRecord);
        logger.info("{}", noKeyRecord);

        // 키 있는 레코드
        ProducerRecord<String, String> keyRecord = new ProducerRecord<>(TOPIC_NAME, "messageKey", "messageValue");
        producer.send(keyRecord);
        logger.info("{}", keyRecord);

        // 파티션 번호까지 직접 지정하고 싶은 경우
        int partitionNumber = 0;
        ProducerRecord<String, String> keyRecordToSpecifyPartition = new ProducerRecord<>(TOPIC_NAME, partitionNumber, "messageKey", "messageValue");
        producer.send(keyRecordToSpecifyPartition);
        logger.info("{}", keyRecordToSpecifyPartition);


        // flush 후 애플리케이션 안전 종료
        producer.flush();
        producer.close();
    }


/**
 * 로그
 * */
/*
[main] INFO org.apache.kafka.clients.producer.ProducerConfig - ProducerConfig values:
	acks = 1
	batch.size = 16384
	bootstrap.servers = [3.34.130.109:9092]
	buffer.memory = 33554432
	client.dns.lookup = default
	client.id = producer-1
	compression.type = none
	connections.max.idle.ms = 540000
	delivery.timeout.ms = 120000
	enable.idempotence = false
	interceptor.classes = []
	key.serializer = class org.apache.kafka.common.serialization.StringSerializer
	linger.ms = 0
	max.block.ms = 60000
	max.in.flight.requests.per.connection = 5
	max.request.size = 1048576
	metadata.max.age.ms = 300000
	metadata.max.idle.ms = 300000
	metric.reporters = []
	metrics.num.samples = 2
	metrics.recording.level = INFO
	metrics.sample.window.ms = 30000
	partitioner.class = class org.apache.kafka.clients.producer.internals.DefaultPartitioner
	receive.buffer.bytes = 32768
	reconnect.backoff.max.ms = 1000
	reconnect.backoff.ms = 50
	request.timeout.ms = 30000
	retries = 2147483647
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
	transaction.timeout.ms = 60000
	transactional.id = null
	value.serializer = class org.apache.kafka.common.serialization.StringSerializer

[main] INFO org.apache.kafka.common.utils.AppInfoParser - Kafka version: 2.5.0
[main] INFO org.apache.kafka.common.utils.AppInfoParser - Kafka commitId: 66563e712b0b9f84
[main] INFO org.apache.kafka.common.utils.AppInfoParser - Kafka startTimeMs: 1643811018824
[kafka-producer-network-thread | producer-1] INFO org.apache.kafka.clients.Metadata - [Producer clientId=producer-1] Cluster ID: lnj0AvC6RwWfwnqu6M51xg
[main] INFO producer.SimpleProducer - ProducerRecord(topic=hello.kafka, partition=null, headers=RecordHeaders(headers = [], isReadOnly = true), key=null, value=message, timestamp=null)
[main] INFO org.apache.kafka.clients.producer.KafkaProducer - [Producer clientId=producer-1] Closing the Kafka producer with timeoutMillis = 9223372036854775807 ms.
*/
}
