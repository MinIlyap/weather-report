package com.prilepskij.i.weather_report;

import com.prilepskij.i.weather_report.consumer.WeatherConsumer;
import com.prilepskij.i.weather_report.producer.WeatherProducer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class WeatherReportApplication {

	public static void main(String[] args) {
		SpringApplication.run(WeatherReportApplication.class, args);
	}

	@Bean
	public CommandLineRunner commandLineRunner(WeatherProducer producer) {
		return args -> {
			Thread.sleep(1000);

			System.out.println("Launching producer...");
			producer.produceWeatherData();
		};
	}
}
