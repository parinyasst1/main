package th.co.solar.solarapi.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class ValueUnit {
    @JsonProperty("Value")
    private Integer Value;
    @JsonProperty("Unit")
    private String Unit;
}
