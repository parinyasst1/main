package th.co.solar.solarapi.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Header {
    @JsonProperty("Title")
    private String Title;
    @JsonProperty("Description")
    private String Description;
    @JsonProperty("Uri")
    private String Uri;
    @JsonProperty("LastBuiltDate")
    private String LastBuiltDate;
    @JsonProperty("CopyRight")
    private String CopyRight;
    @JsonProperty("Generator")
    private String Generator;
}
