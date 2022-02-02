package consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 명시적 비동기 커밋 예제
 * */
public class ConsumerWithAsyncCommitAll {
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
        // 명시적 커밋을 하려면 이 설정 필수
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        // 컨슈머가 토픽 구독
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        while (true) {
            // 반복 가능한 객체로 되어 있음
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            // 커밋을 위한 offset 정보

            for (ConsumerRecord<String, String> record : records) {
                // ConsumerRecord(topic = hello.kafka, partition = 2, leaderEpoch = 0, offset = 2, CreateTime = 1643813098898, serialized key size = -1, serialized value size = 11, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = testMessage)
                logger.info("record: {}", record);
                // 커밋을 위한 설정 정보 세팅
            }

            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {

                    if (exception != null) {
                        logger.error("commit fail {}", offsets, exception);
                    } else {
                        logger.info("commit succeeded");
                    }
                }
            });
        }
    }
}
