package com.prilepskij.i.weather_report.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.prilepskij.i.weather_report.model.WeatherData;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

@Component
public class WeatherProducer {
    private static final Logger logger = LoggerFactory.getLogger(WeatherProducer.class);
    private final KafkaProducer<String, String> kafkaProducer;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String topic;
    private final Random random = new Random();
    private final List<String> cities = Arrays.asList("Магадан", "Чукотка", "Санкт-Петербург", "Тюмень");
    private final List<String> conditions = Arrays.asList("солнечно", "облачно", "дождь");
    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
    private final CountDownLatch producerLatch = new CountDownLatch(1);

    public WeatherProducer(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                           @Value("${spring.kafka.template.default-topic}") String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.kafkaProducer = new KafkaProducer<>(props);
        this.objectMapper.registerModule(new JavaTimeModule());
        logger.info("WeatherProducer initialized with topic: {}", topic);
    }

    public void produceWeatherData() {
        try {
            // Генерируем данные за неделю
            LocalDate startDate = LocalDate.now().minusDays(6);
            for (int i = 0; i < 7; i++) {
                LocalDate eventDate = startDate.plusDays(i);
                for (String city : cities) {
                    int temperature = random.nextInt(36);
                    String condition = conditions.get(random.nextInt(conditions.size()));
                    WeatherData weatherData = new WeatherData(city, temperature, condition, eventDate);

                    // Сериализуем в JSON
                    String jsonValue;
                    try {
                        jsonValue = objectMapper.writeValueAsString(weatherData);
                    } catch (JsonProcessingException e) {
                        logger.error("Failed to serialize WeatherData for city {} on date {}: {}",
                                city, eventDate.format(dateFormatter), e.getMessage());
                        continue;
                    }

                    // Отправляем в Kafka
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, city, jsonValue);
                    kafkaProducer.send(record, (metadata, exception) -> {
                        if (exception == null) {
                            logger.info("Sent message: city={}, date={}, topic={}, partition={}, offset={}",
                                    city, eventDate.format(dateFormatter), metadata.topic(),
                                    metadata.partition(), metadata.offset());
                        } else {
                            logger.error("Failed to send message for city {} on date {}: {}",
                                    city, eventDate.format(dateFormatter), exception.getMessage());
                        }
                    });
                }
            }

        } catch (Exception e) {
            logger.error("Unexpected error in produceWeatherData: {}", e.getMessage());
        } finally {
            producerLatch.countDown(); // Сигнализируем, что отправка завершена
            logger.info("WeatherProducer completed sending all messages");
        }
    }

    public CountDownLatch getProducerLatch() {
        return producerLatch;
    }
}