package com.prilepskij.i.weather_report.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.prilepskij.i.weather_report.model.WeatherData;
import com.prilepskij.i.weather_report.producer.WeatherProducer;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Component
public class WeatherConsumer {
    private static final Logger logger = LoggerFactory.getLogger(WeatherConsumer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
    private final WeatherProducer producer;
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final List<WeatherData> messages = new ArrayList<>();
    private static final int EXPECTED_MESSAGES = 28; // 4 города * 7 дней
    private final List<String> cities = Arrays.asList("Магадан", "Чукотка", "Санкт-Петербург", "Тюмень");

    // Статистика
    private final Map<String, Integer> rainyDaysByCity = new HashMap<>();
    private final Map<String, Integer> sunnyDaysByCity = new HashMap<>();
    private final Map<String, Integer> maxTempByCity = new HashMap<>();
    private final Map<String, LocalDate> maxTempDateByCity = new HashMap<>();
    private final Map<String, List<Integer>> tempByCity = new HashMap<>();

    public WeatherConsumer(WeatherProducer producer,
                           @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                           @Value("${spring.kafka.consumer.group-id}") String groupId,
                           @Value("${spring.kafka.template.default-topic}") String topic,
                           @Value("${spring.kafka.consumer.auto-offset-reset}") String offsetResetConfig) {
        this.producer = producer;
        this.objectMapper.registerModule(new JavaTimeModule());

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetConfig);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this.kafkaConsumer = new KafkaConsumer<>(props);
        this.kafkaConsumer.subscribe(Arrays.asList(topic));
        logger.info("WeatherConsumer initialized with topic: {}", topic);
    }

    @PostConstruct
    public void startConsuming() {
        new Thread(() -> {
            try {
                // Ждем завершения работы продюсера
                producer.getProducerLatch().await();

                while (!Thread.currentThread().isInterrupted()) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                    for (var record : records) {
                        try {
                            WeatherData weatherData = objectMapper.readValue(record.value(), WeatherData.class);
                            logger.info("Received message: city={}, date={}, temperature={}, condition={}",
                                    weatherData.getCity(), weatherData.getDate().format(dateFormatter),
                                    weatherData.getTemperature(), weatherData.getCondition());

                            synchronized (messages) {
                                messages.add(weatherData);
                                processWeatherData(weatherData);

                                // Выводим статистику после получения всех ожидаемых сообщений
                                if (messages.size() >= EXPECTED_MESSAGES) {
                                    printStatistics();
                                    // Сбрасываем данные для следующего запуска
                                    messages.clear();
                                    rainyDaysByCity.clear();
                                    sunnyDaysByCity.clear();
                                    maxTempByCity.clear();
                                    maxTempDateByCity.clear();
                                    tempByCity.clear();
                                    break; // Выходим после обработки всех сообщений
                                }
                            }
                        } catch (Exception e) {
                            logger.error("Failed to deserialize or process message: value={}, error={}",
                                    record.value(), e.getMessage());
                        }
                    }
                    kafkaConsumer.commitSync(); // Отмечает прочитанные сообщения
                }
            } catch (InterruptedException e) {
                logger.error("Consumer thread interrupted: {}", e.getMessage());
                Thread.currentThread().interrupt();
            } finally {
                kafkaConsumer.close();
                logger.info("KafkaConsumer closed");
            }
        }).start();
    }

    private void processWeatherData(WeatherData data) {
        String city = data.getCity();
        String condition = data.getCondition();
        int temperature = data.getTemperature();

        // Подсчет дождливых и солнечных дней
        rainyDaysByCity.put(city, rainyDaysByCity.getOrDefault(city, 0) + (condition.equals("дождь") ? 1 : 0));
        sunnyDaysByCity.put(city, sunnyDaysByCity.getOrDefault(city, 0) + (condition.equals("солнечно") ? 1 : 0));

        // Самая высокая температура
        if (!maxTempByCity.containsKey(city) || temperature > maxTempByCity.get(city)) {
            maxTempByCity.put(city, temperature);
            maxTempDateByCity.put(city, data.getDate());
        }

        // Средняя температура
        tempByCity.computeIfAbsent(city, k -> new ArrayList<>()).add(temperature);
    }

    private void printStatistics() {
        logger.info("\n=== Статистика за неделю ===");
        for (String city : cities) {
            logger.info("--- Город {} ---", city);
            logger.info("Дождливых дней = {}, солнечных дней = {}",
                    rainyDaysByCity.getOrDefault(city, 0), sunnyDaysByCity.getOrDefault(city, 0));
            if (maxTempByCity.containsKey(city)) {
                logger.info("Самая высокая температура {}°C была {}",
                        maxTempByCity.get(city), maxTempDateByCity.get(city).format(dateFormatter));
            }
            if (tempByCity.containsKey(city)) {
                List<Integer> temps = tempByCity.get(city);
                double avgTemp = temps.stream().mapToDouble(Integer::doubleValue).average().orElse(0.0);
                logger.info("Средняя температура {}°C", Math.round(avgTemp));
            }
        }
    }
}
