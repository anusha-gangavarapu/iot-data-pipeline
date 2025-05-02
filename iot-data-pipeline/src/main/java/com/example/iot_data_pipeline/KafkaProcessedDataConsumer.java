package com.example.iot_data_pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class KafkaProcessedDataConsumer {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaProcessedDataConsumer.class);
	
	private final SensorDataRepository repository;
	private final ObjectMapper objectMapper;
	KafkaProcessedDataConsumer(SensorDataRepository repository, ObjectMapper objectMapper){
		this.repository = repository;
		this.objectMapper = objectMapper;
	}
	
	@KafkaListener(topics = "${app.kafka.processed.topic}", groupId = "kafka-demo-group")
	public void consume(String message) {
		SensorData data = null;
		try {
			logger.info("Received message from sensor_processed_data topic, saving it to db");
			data = objectMapper.readValue(message, SensorData.class);
			repository.save(data);
			logger.info("Successfully saved to DB: {}", data);
		} catch (Exception e) {
			logger.error("Failed to process message: {}", data, e);
		}
	}
	
}
