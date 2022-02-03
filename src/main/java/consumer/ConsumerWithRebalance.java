package consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * 명시적 커밋 예제
 *
 * 리밸런싱 대처 코드.
 * 컨슈머 안전 종료 코드
 * */
public class ConsumerWithRebalance {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    // 토픽명
    private final static String TOPIC_NAME = "hello.kafka";
    // 컨슈머 그룹명 => 구독형태로 사용하면 꼭 필요
    private final static String GROUP_ID = "hello-group";
    // 카프카 클러스터 주소
    private final static String BOOTSTRAP_SERVERS = "3.34.130.109:9092";

    private static KafkaConsumer<String, String> consumer;

    private static Map<TopicPartition, OffsetAndMetadata> currentOffset;

    public static void main(String[] args) {
        // 런타임에 훅을 넣어주어야 합니다!
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());

        // 프로퍼티 생성
        Properties configs = new Properties();

        // 필수 값
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        // 명시적 커밋을 하려면 이 설정 필수
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);


        consumer = new KafkaConsumer<>(configs);
        // 컨슈머가 토픽 구독 (리밸런싱 대처를 위한 클래스)
        consumer.subscribe(Arrays.asList(TOPIC_NAME), new RebalanceListener());

        try {
            while (true) {
                // 반복 가능한 객체로 되어 있음
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                // 커밋을 위한 offset 정보
                currentOffset = new HashMap<>();

                for (ConsumerRecord<String, String> record : records) {
                    // ConsumerRecord(topic = hello.kafka, partition = 2, leaderEpoch = 0, offset = 2, CreateTime = 1643813098898, serialized key size = -1, serialized value size = 11, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = testMessage)
                    logger.info("record: {}", record);
                    // 커밋을 위한 설정 정보 세팅
                    currentOffset.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, null)
                    );

                    consumer.commitSync(currentOffset);
                }
            }
        } catch (WakeupException e) {
            logger.warn("wakeup consumer");
            // 리소스 종료 처리하기
        } finally {
            consumer.close();
        }
    }

    // 리밸런싱 리스너
    private static class RebalanceListener implements ConsumerRebalanceListener {

        // 리밸런싱이 일어나기 전에
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.warn("partitions are assigned");
            consumer.commitSync(currentOffset);
        }

        // 리밸런싱이 끝나고
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.warn("partition are revoked");

        }
    }

    // 안전종료 대처 코드
    static class ShutdownThread extends Thread {
        @Override
        public void run() {
            logger.info("shutdown hook");
            consumer.wakeup();
        }
    }
}
