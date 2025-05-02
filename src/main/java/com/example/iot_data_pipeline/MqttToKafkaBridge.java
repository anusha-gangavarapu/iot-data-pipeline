package com.example.iot_data_pipeline;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Service
public class MqttToKafkaBridge implements MqttCallbackExtended {
    
    private static final Logger logger = LoggerFactory.getLogger(MqttToKafkaBridge.class);
    
    @Value("${mqtt.broker-url}")
    private String brokerUrl;
    
    @Value("${mqtt.topic}")
    private String topic;
    
    @Value("${mqtt.client-id}")
    private String clientId;
    
    @Value("${mqtt.connection-timeout:30}")
    private int connectionTimeout;
    
    @Value("${mqtt.keep-alive-interval:60}")
    private int keepAliveInterval;
    
    @Value("${mqtt.reconnect-delay:5000}")
    private long reconnectDelay;
    
    @Value("${app.kafka.topic}")
    private String topic1;
    
    private MqttAsyncClient mqttClient;
    private final AtomicBoolean isConnected = new AtomicBoolean(false);
    private final AtomicBoolean isSubscribed = new AtomicBoolean(false);
    private final AtomicReference<String> lastError = new AtomicReference<>("No errors");

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    
    @PostConstruct
    public void init() {
        try {
            MemoryPersistence persistence = new MemoryPersistence();
            mqttClient = new MqttAsyncClient(brokerUrl, clientId, persistence);
            mqttClient.setCallback(this);
            connectWithRetry();
        } catch (MqttException e) {
            lastError.set("Initialization failed: " + e.getMessage());
            logger.error("Initialization failed", e);
            scheduleReconnect();
        }
    }

    private void connectWithRetry() {
        try {
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setConnectionTimeout(connectionTimeout);
            options.setKeepAliveInterval(keepAliveInterval);
            options.setMaxReconnectDelay(10000);

            logger.info("Attempting to connect to MQTT broker: {}", brokerUrl);
            IMqttToken token = mqttClient.connect(options);
            token.waitForCompletion(connectionTimeout * 1000L);

            if (token.isComplete() && token.getException() == null) {
                isConnected.set(true);
                logger.info("Successfully connected to MQTT broker");
            } else {
                throw token.getException() != null ? 
                    token.getException() : 
                    new MqttException(MqttException.REASON_CODE_CLIENT_NOT_CONNECTED);
            }
        } catch (MqttException e) {
            lastError.set("Connection failed: " + e.getMessage());
            logger.error("Connection failed to broker: {}", brokerUrl, e);
            isConnected.set(false);
            scheduleReconnect();
        }
    }

    @Override
    public void connectComplete(boolean reconnect, String serverURI) {
        logger.info("Connection {}established to {}", reconnect ? "re" : "", serverURI);
        isConnected.set(true);
        resetSubscription();
    }

    private void resetSubscription() {
        try {
            if (isSubscribed.get()) {
                mqttClient.unsubscribe(topic);
                isSubscribed.set(false);
                logger.info("Unsubscribed from topic for reset: {}", topic);
            }
            subscribeToTopic();
        } catch (MqttException e) {
            lastError.set("Subscription reset failed: " + e.getMessage());
            logger.error("Error resetting subscription", e);
        }
    }

    private void subscribeToTopic() {
        if (!isConnected.get() || isSubscribed.get()) {
            return;
        }

        try {
            logger.info("Attempting to subscribe to topic: {}", topic);
            mqttClient.subscribe(topic, 1, null, new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken asyncActionToken) {
                    isSubscribed.set(true);
                    //logger.info("Subscription confirmed for topic: {}", topic);
                }

                @Override
                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    lastError.set("Subscription failed: " + exception.getMessage());
                    logger.error("Subscription failed for topic: {}", topic, exception);
                    isSubscribed.set(false);
                    scheduleReconnect();
                }
            });
        } catch (MqttException e) {
            lastError.set("Subscription error: " + e.getMessage());
            logger.error("Subscription exception for topic: {}", topic, e);
            isSubscribed.set(false);
            scheduleReconnect();
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        lastError.set("Connection lost: " + cause.getMessage());
        logger.warn("MQTT connection lost", cause);
        isConnected.set(false);
        isSubscribed.set(false);
        scheduleReconnect();
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
      //  final long startTime = System.currentTimeMillis();
        logger.debug("Message arrived on topic: {} message : {}]", 
            topic, message);

        try {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            logger.info("Message arrived on topic: {} message: {}", topic, payload);

            kafkaTemplate.send(topic1, payload);
            logger.info("Message Sent to Kafka Sensor_raw_topic");
        } 
        catch (Exception e) {
            lastError.set("Processing error: " + e.getMessage());
            logger.error("Unexpected error processing message", e);
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        logger.debug("Message delivery complete");
    }

    private void scheduleReconnect() {
        try {
            logger.info("Scheduling reconnect in {}ms", reconnectDelay);
            Thread.sleep(reconnectDelay);
            if (!isConnected.get()) {
                connectWithRetry();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            lastError.set("Reconnect interrupted: " + e.getMessage());
            logger.warn("Reconnect thread interrupted");
        } catch (Exception e) {
            lastError.set("Reconnect failed: " + e.getMessage());
            logger.error("Reconnect attempt failed", e);
            scheduleReconnect(); // Continue trying
        }
    }

    @PreDestroy
    public void cleanup() {
        try {
            if (mqttClient != null) {
                if (mqttClient.isConnected()) {
                    mqttClient.disconnect();
                    logger.info("Disconnected from MQTT broker");
                }
                mqttClient.close();
            }
        } catch (MqttException e) {
            logger.warn("Error during disconnection", e);
        } finally {
            isConnected.set(false);
            isSubscribed.set(false);
        }
    }

    // Status monitoring methods
    public boolean isConnected() {
        return mqttClient != null && mqttClient.isConnected();
    }

    public boolean isSubscribed() {
        return isSubscribed.get();
    }

    public String getCurrentTopic() {
        return topic;
    }

    public String getLastError() {
        return lastError.get();
    }
}