package th.co.solar.solarapi.model;

import lombok.Data;

@Data
public class TotalSite {
    private String gridkwTall;
    private String LoadkwTall;
    private String solartotalinputall;
    private String solartotaloutputall;
    private String persengridall;
    private String persenpvall;
    private String persensolarall;

}
