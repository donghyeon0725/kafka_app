package producer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ConfluentSimpleProducer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    // 토픽명
    private final static String TOPIC_NAME = "hello.kafka";
    // 카프카 클러스터 주소
    private final static String BOOTSTRAP_SERVERS = "pkc-ymrq7.us-east-2.aws.confluent.cloud:9092";


    public static void createTopic(final String topic, final Properties cloudConfig) {

        final NewTopic newTopic = new NewTopic(topic, Optional.empty(), Optional.empty());
        try (final AdminClient adminClient = AdminClient.create(cloudConfig)) {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            // Ignore if TopicExistsException, which may be valid if topic exists
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) {
        // 프로퍼티 생성
        Properties configs = new Properties();

        // 필수 값
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        configs.put("basic.auth.user.info", "ASVJOHF6BFSPBXOC:1H4rZL8gm3ye7Z3KhdG32WqKZK5G+r6P8Hmt84m68qIjLfQS1WqSbWjshxVPFUg4");

        // 인증을 위한 값 Successfully logged in
        configs.put("security.protocol", "SASL_SSL");
        configs.put("sasl.mechanism", "PLAIN");
        configs.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule   required username='여기에 pub 키를 입력하세요'   password='여기에 pri 키를 입력하세요';");

        // 토픽 생성
        createTopic(TOPIC_NAME, configs);

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
}
