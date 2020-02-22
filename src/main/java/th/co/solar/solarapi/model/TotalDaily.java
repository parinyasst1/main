package th.co.solar.solarapi.model;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class TotalDaily {
    private String chartdailyTotalgrid;
    private String chartdailyTotalinv1pv;
    private String chartdailyTotalinv1solar;
    private String chartdailyTotalinv2pv;
    private String chartdailyTotalinv2solar;
    private String chartdailyTotalinv3pv;
    private String chartdailyTotalinv3solar;
    private String chartdailyTotalinv4pv;
    private String chartdailyTotalinv4solar;
    private String chartdailyTotalinv5pv;
    private String chartdailyTotalinv5solar;
    private String chartdailyTotalload;
    private String chartdailyTotalpv;
    private String chartdailyTotalsolar;
    private String date;
}
