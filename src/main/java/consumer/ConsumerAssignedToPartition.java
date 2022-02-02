package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

/**
 * 파티션에 할당된 컨슈머
 * */
public class ConsumerAssignedToPartition {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    // 토픽명
    private final static String TOPIC_NAME = "hello.kafka";
    // 컨슈머 그룹명 => 구독형태로 사용하면 꼭 필요
    private final static String GROUP_ID = "hello-group";
    // 카프카 클러스터 주소
    private final static String BOOTSTRAP_SERVERS = "3.34.130.109:9092";
    // 할당할 파티션 번호
    private final static int PARTITION_NUMBER = 0;

    public static void main(String[] args) {
        // 프로퍼티 생성
        Properties configs = new Properties();

        // 필수 값
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        // 파티션에 직접 할당
        consumer.assign(Collections.singleton(new TopicPartition(TOPIC_NAME, PARTITION_NUMBER)));

        // 컨슈머에 할당된 파티션 확인
        Set<TopicPartition> assignedTopicPartition = consumer.assignment();
        // [main] INFO consumer.SimpleConsumer - [hello.kafka-0]
        logger.info("{}", assignedTopicPartition);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("{}", record);
            }
        }
    }
}
