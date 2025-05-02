package com.example.iot_data_pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class KafkaRawMessageConsumerAndPublisher {

	private static final Logger logger = LoggerFactory.getLogger(KafkaRawMessageConsumerAndPublisher.class);
	
	private final ObjectMapper objectMapper;
	@Value("${app.kafka.topic}")
	private String topicName;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	  
	public KafkaRawMessageConsumerAndPublisher(SensorDataRepository repository, ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@KafkaListener(topics = "${app.kafka.topic}", groupId = "kafka-demo-group")
	public void consume(String message) {
		SensorData data = null;
		try {
			logger.info("Received message on topic {} message {} " + topicName , message);
			data = objectMapper.readValue(message, SensorData.class);
			if(data.getTemparature() > 35) {
				logger.info("Temparature greater than 35 publishing to Sensor_processed_data topic", data);
				publishToKafkaProcessedTopic(message);
			}
			
		} catch (Exception e) {
			logger.error("Failed to process message: {}", data, e);
		}
	}
	
	public void publishToKafkaProcessedTopic(String message) {
		logger.info("Received message on topic sensor_processed_data message {} " + message);
		  kafkaTemplate.send("sensor_processed_data", message);
	}

}