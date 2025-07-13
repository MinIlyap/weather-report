package com.prilepskij.i.weather_report.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDate;

public class WeatherData {
    @JsonProperty("city")
    private String city;

    @JsonProperty("temperature")
    private int temperature;

    @JsonProperty("condition")
    private String condition;

    @JsonProperty("date")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy")
    private LocalDate date;

    public WeatherData() {
    }

    public WeatherData(String city, int temperature, String condition, LocalDate date) {
        this.city = city;
        this.temperature = temperature;
        this.condition = condition;
        this.date = date;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public LocalDate getDate() {
        return date;
    }

    public void setDate(LocalDate date) {
        this.date = date;
    }
}