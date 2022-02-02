package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 커스텀 파티셔너를 사용한 프로듀서 애플리케이션
 *
 * 전송 여부 확인 예제 포함
 * */
public class ProducerWithCustomPartitioner {
    private final static Logger logger = LoggerFactory.getLogger(ProducerWithCustomPartitioner.class);
    // 토픽명
    private final static String TOPIC_NAME = "hello.kafka";
    // 카프카 클러스터 주소
    private final static String BOOTSTRAP_SERVERS = "3.34.130.109:9092";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 프로퍼티 생성
        Properties configs = new Properties();

        // 필수 값
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 커스텀 파티셔너 장착
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);

        // 프로듀서 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        ProducerRecord<String, String> record1 = new ProducerRecord<>(TOPIC_NAME, "messageKey", "messageValue");
        ProducerRecord<String, String> record2 = new ProducerRecord<>(TOPIC_NAME, "messageKey", "messageValue");

        // 동기로 결과 리턴 기다림
        RecordMetadata recordMetadata1 = producer.send(record1).get();
        // hello.kafka-0@6 => hello.kafka 토픽의 0번 파티션에 전송되었고 6번 오프셋을 가지고 있음을 알 수 있음
        logger.info(recordMetadata1.toString());

        // 비동기로 결과를 받아봄 / hello.kafka-0@7
        producer.send(record1, new ProducerCallback());

        producer.flush();
        producer.close();
    }
}
