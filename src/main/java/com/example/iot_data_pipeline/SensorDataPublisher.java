package com.example.iot_data_pipeline;

import java.time.LocalDateTime;
import java.util.Random;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class SensorDataPublisher {

    private static final String BROKER_URL = "tcp://localhost:1883";
    private static final String TOPIC = "iot/sensors";
    private static final String CLIENT_ID = "sensor-publisher";
    
    private final MqttClient mqttClient;
    private final Random random = new Random();
    
    public SensorDataPublisher() throws MqttException {
        this.mqttClient = new MqttClient(BROKER_URL, CLIENT_ID, new MemoryPersistence());
        connectToMqttBroker();
    }
    
    private void connectToMqttBroker() throws MqttException {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        mqttClient.connect(options);
    }
    
    @Scheduled(fixedRate = 50000) 
    public void publishSensorData() {
    //	System.out.println("****scheduling is working*****");
        try {
            if (!mqttClient.isConnected()) {
                connectToMqttBroker();
            }
            
            String sensorId = "device-" + (random.nextInt(5) + 1); // sensor-1 to sensor-5
            double value = 20 + random.nextDouble() * 15; // Random value between 20-35
            String timestamp = LocalDateTime.now().toString();
            float temparature = 35 + random.nextFloat();
            
            String payload = String.format(
                "{\"device_id\":\"%s\",\"humidity\":%.2f,\"temparature\":%.2f,\"timestamp\":\"%s\"}",             
                sensorId, value,temparature,timestamp
            );
            

            
            MqttMessage message = new MqttMessage(payload.getBytes());
            message.setQos(1); // At least once delivery
            
            mqttClient.publish(TOPIC, message);
            System.out.println("Message Sent to MQTT Topic: " + payload);
            
        } catch (MqttException e) {
            System.err.println("Error publishing message: " + e.getMessage());
        }
    }
}