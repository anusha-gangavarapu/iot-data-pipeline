package com.example.iot_data_pipeline;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class IotDataPipelineApplication {

	public static void main(String[] args) {
		SpringApplication.run(IotDataPipelineApplication.class, args);
	}

}
