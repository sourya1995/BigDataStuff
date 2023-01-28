import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class AsyncProducer {
	
	public static void main(String[] args) {
		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", "localhost:9092");
		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer",
	                "org.apache.kafka.common.serialization.StringSerializer");
		
		KafkaProducer producer = new KafkaProducer<String, String>(kafkaProps);
		ProducerRecord<String, String> record = new ProducerRecord<>("datajek-topic", "my-key", "my-value");
		
		try {
			producer.send(record, new ProducerCallback());
			System.out.println("Kafka message sent asynchronously.");

			
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			producer.flush();
			producer.close();
		}
		
		Thread.sleep(3000);
		
	}
	
	private static class ProducerCallback implements Callback{

		public void onCompletion(RecordMetadata metadata, Exception exception) {
			// TODO Auto-generated method stub
			if(exception != null) {
				exception.printStackTrace();
			} else {
				 System.out.println(String.format("Message written to partition %s with offset %s", metadata.partition(),
	                        metadata.offset()));
			}
			
		}
		
	}

}
