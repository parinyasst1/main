package th.co.solar.solarapi.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class SevenDaysForecast {
    @JsonProperty("Date")
    private String Date;
    @JsonProperty("WeatherDescription")
    private String WeatherDescription;
    @JsonProperty("WeatherDescriptionEn")
    private String WeatherDescriptionEn;
    @JsonProperty("WaveHeight")
    private String WaveHeight;
    @JsonProperty("WaveHeightEn")
    private String WaveHeightEn;
    @JsonProperty("TempartureLevel")
    private String TempartureLevel;
    @JsonProperty("TempartureLevelEn")
    private String TempartureLevelEn;
    @JsonProperty("MaxTemperature")
    private ValueUnit MaxTemperature;
    @JsonProperty("MinTemperature")
    private ValueUnit MinTemperature;
    @JsonProperty("WindDirection")
    private ValueUnit WindDirection;
    @JsonProperty("WindSpeed")
    private ValueUnit WindSpeed;
    @JsonProperty("Rain")
    private ValueUnit Rain;
}
