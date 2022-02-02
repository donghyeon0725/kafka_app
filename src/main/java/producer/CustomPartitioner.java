package producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * 파티셔너를 직접 구현하여, 토픽의 파티셔너에 메세지를 배치하는 방법을 변경하고 싶을 때 직접 구현
 * */
public class CustomPartitioner implements Partitioner {

    // 파티셔닝 하는 메소드
    // 메세지를 받아서, 할당할 파티션의 번호 리턴
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        // 키가 null 일 때
        if (keyBytes == null) {
            throw new InvalidRecordException("Need message key");
        }

        // 키 값이 messageKey 일 때 0번 할당
        if (((String)key).equals("messageKey")) {
            return 0;
        }

        // 키의 헤시 값에 따라서 파티션 할당
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int partitionCount = partitions.size();
        return Utils.toPositive(Utils.murmur2(keyBytes)) % partitionCount;
    }

    // 파티셔너 종료 시점에 호출
    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
