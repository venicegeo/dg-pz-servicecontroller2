package org.venice.piazza.serviceregistry.controller.messaging;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ser.std.StringSerializer;
//import kafka.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestExecuteService {
	private KafkaProducer<String, String> producer = null;
	private  String topic = "Register-Service-Job";
	private final Properties props = new Properties();
	
	@Before
	public void Setup() {
		/*props.put("key.serializer", StringSerializer.class);
		props.put("value.serializer", StringSerializer.class);
	    props.put("bootstrap.servers", "kafka.dev:9092");
	    props.put("timeout.ms","10000L");
	    props.put("ack", "1");

        // how many times to retry when produce request fails?
        props.put("retries", "3");
        props.put("linger.ms", 5);
        ProducerConfig config = new ProducerConfig(props);
		producer = new KafkaProducer<String, String>(props);*/
		
	}

	@Test
	public void KafkaConsumerTest() {
		String upperServiceDef = "{  \"name\":\"toUpper Params\"," +
        "\"description\":\"Service to convert string to uppercase\"," + 
        "\"url\":\"http://localhost:8082/string/toUpper\"," + 
         "\"method\":\"POST\"," +
         "\"params\": [\"aString\"]," + 
         "\"mimeType\":\"application/json\"" +
       "}";
		String result = "error";
		/*producer.send(new ProducerRecord<String, String>(topic, upperServiceDef),
				new Callback() {
			@Override
	        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
	            if (e != null) {
	                System.out.println("Error producing to topic " + recordMetadata.topic());
	                e.printStackTrace();
	            }
	            else {
	            	System.out.println("Success");
	            }
	        }
			
		});*/
		producer.send(new ProducerRecord<String, String>(topic, upperServiceDef));
	}

}
