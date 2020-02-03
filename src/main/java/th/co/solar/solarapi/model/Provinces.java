package th.co.solar.solarapi.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class Provinces {
    @JsonProperty("ProvinceNameTh")
    private String ProvinceNameTh;
    @JsonProperty("ProvinceNameEng")
    private String ProvinceNameEng;
    @JsonProperty("SevenDaysForecast")
    private List<SevenDaysForecast> SevenDaysForecast;
}
