package com.example.iot_data_pipeline;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Value("${app.topic}")
    private String kafkaTopicName;
    
    @Value("${mqtt.topic}")
    private String mqttTopicName;

    @Bean
    public NewTopic sensorTopic() {
        return TopicBuilder.name(mqttTopicName)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic sensorKafkaTopic() {
        return TopicBuilder.name(kafkaTopicName)
                .partitions(3)
                .replicas(1)
                .build();
    }
}