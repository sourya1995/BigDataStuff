import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

public class CustomPartitioner implements Partitioner{

	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub
		
	}

	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		// TODO Auto-generated method stub
		List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
		int numPartitions = partitions.size();
		
		if((keyBytes == null) || (!(key instanceof String))) {
			throw new InvalidRecordException("Message is missing the key");
		}
		
		if(((String)key).equals("USA")){
			return numPartitions;
		}
		return (Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1));
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}
	
}
