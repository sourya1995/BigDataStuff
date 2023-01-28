import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Producer {
	
	public static void main(String[] args) {
		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", "localhost:9092");
		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer",
	                "org.apache.kafka.common.serialization.StringSerializer");
		
		KafkaProducer producer = new KafkaProducer<String, String>(kafkaProps);
		ProducerRecord<String, String> record = new ProducerRecord<>("datajek-topic", "my-key", "my-value");
		
		try {
			Future<RecordMetadata> future = producer.send(record);
			RecordMetadata recordMetadata = future.get();
			 System.out.println(String.format("Message written to partition %s with offset %s", recordMetadata.partition(),
	                    recordMetadata.offset()));
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			producer.flush();
			producer.close();
		}
		
	}

}
