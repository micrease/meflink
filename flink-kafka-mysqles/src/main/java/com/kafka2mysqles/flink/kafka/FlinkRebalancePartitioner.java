package com.kafka2mysqles.flink.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.Preconditions;

import java.nio.charset.StandardCharsets;

@PublicEvolving
public class FlinkRebalancePartitioner<T> extends FlinkKafkaPartitioner<T> {
    private static final long serialVersionUID = -3785320239953858777L;
    private int parallelInstanceId;

    public FlinkRebalancePartitioner() {
    }

    public void open(int parallelInstanceId, int parallelInstances) {
        Preconditions.checkArgument(parallelInstanceId >= 0, "Id of this subtask cannot be negative.");
        Preconditions.checkArgument(parallelInstances > 0, "Number of subtasks must be larger than 0.");
        this.parallelInstanceId = parallelInstanceId;
    }

    public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        Preconditions.checkArgument(partitions != null && partitions.length > 0, "Partitions of the target topic is empty.");
        String after = JSON.parseObject(new String(value, StandardCharsets.UTF_8)).getString("after");
        int id = JSON.parseObject(after).getBigInteger("id").intValue();
        return partitions[id % partitions.length];
    }

    public boolean equals(Object o) {
        return this == o || o instanceof FlinkRebalancePartitioner;
    }

    public int hashCode() {
        return FlinkRebalancePartitioner.class.hashCode();
    }
}
