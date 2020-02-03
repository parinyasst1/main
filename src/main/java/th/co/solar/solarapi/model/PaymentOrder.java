package th.co.solar.solarapi.model;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Locale;

@Builder
@Data
public class PaymentOrder {
    private String bankCode;
    private BigDecimal value;
    private LocalDate localDate;
}
