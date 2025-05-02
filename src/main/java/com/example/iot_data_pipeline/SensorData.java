package com.example.iot_data_pipeline;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class SensorData {
	@Id
	private String device_id;
	private double humidity;
	private float temparature;
	private String timestamp;
	
	public String getDevice_id() {
		return device_id;
	}


	public void setDevice_id(String device_id) {
		this.device_id = device_id;
	}


	public double getHumidity() {
		return humidity;
	}


	public void setHumidity(double humidity) {
		this.humidity = humidity;
	}


	public float getTemparature() {
		return temparature;
	}


	public void setTemparature(float temparature) {
		this.temparature = temparature;
	}


	public String getTimestamp() {
		return timestamp;
	}


	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}


	@Override
	public String toString() {
		return "SensorData{" + "device_id='" + device_id + '\'' + ", humidity=" + humidity + '}';
	}

}