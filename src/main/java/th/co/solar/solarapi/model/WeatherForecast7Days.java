package th.co.solar.solarapi.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class WeatherForecast7Days {
    @JsonProperty("Provinces")
    private List<Provinces> Provinces;
    @JsonProperty("Header")
    private Header Header;
}

