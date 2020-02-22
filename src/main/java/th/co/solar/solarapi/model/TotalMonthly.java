package th.co.solar.solarapi.model;

import lombok.Data;

@Data
public class TotalMonthly {
    private String chartmonthlyTotalgrid;
    private String chartmonthlyTotalinv1pv;
    private String chartmonthlyTotalinv1solar;
    private String chartmonthlyTotalinv2pv;
    private String chartmonthlyTotalinv2solar;
    private String chartmonthlyTotalinv3pv;
    private String chartmonthlyTotalinv3solar;
    private String chartmonthlyTotalinv4pv;
    private String chartmonthlyTotalinv4solar;
    private String chartmonthlyTotalinv5pv;
    private String chartmonthlyTotalinv5solar;
    private String chartmonthlyTotalload;
    private String chartmonthlyTotalpv;
    private String chartmonthlyTotalsolar;
    private String date;
}
