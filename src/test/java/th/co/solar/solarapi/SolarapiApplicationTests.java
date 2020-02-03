package th.co.solar.solarapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import th.co.solar.solarapi.model.PaymentOrder;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;

@Slf4j
@SpringBootTest
class SolarapiApplicationTests {

	@Test
	void contextLoads() {
	}

	@Test
	public void test1() throws JsonProcessingException {
		BigDecimal c = BigDecimal.ZERO;
		BigDecimal a1 = new BigDecimal("4");
		BigDecimal a2 = new BigDecimal("-2.9");
		BigDecimal a3 = new BigDecimal("0");
		BigDecimal a4 = new BigDecimal("-1.8");
		BigDecimal a5 = new BigDecimal("0.2");
		BigDecimal a6 = new BigDecimal("0");
		c = c.add(a1);
		c = c.add(a2);
		c = c.add(a3);
		c = c.add(a4);
		c = c.add(a5);
		c = c.add(a6);
		c = c.setScale(1, BigDecimal.ROUND_HALF_UP);
		log.info(c.toString());
	}

	@Test
	public void test2() throws JsonProcessingException {
		Object object = -4.1;
		BigDecimal s = convertObjectToBigDecimal(object);
		log.info(s.toString());
	}

	public BigDecimal convertObjectToBigDecimal(Object object){
		BigDecimal result = BigDecimal.ZERO;
		if(object instanceof String){
			result = new BigDecimal((String)object);
		}else if(object instanceof Long){
			result = new BigDecimal((Long)object);
		}else if(object instanceof Integer){
			result = new BigDecimal((Integer)object);
		}else if(object instanceof Float){
			result = new BigDecimal((Float)object);
		}else if(object instanceof Double){
			result = new BigDecimal((Double)object);
		}else if(object instanceof Integer){
			result = new BigDecimal((Integer)object);
		}
		return result;
	}

	@Test
	public void test() throws JsonProcessingException {
		List<PaymentOrder> paymentOrderList = new ArrayList<>();
		PaymentOrder paymentOrder = PaymentOrder.builder()
				.bankCode("CIMB")
				.localDate(LocalDate.of(2020, Month.JANUARY, 1))
				.value(new BigDecimal("1"))
				.build();
		paymentOrderList.add(paymentOrder);
		paymentOrder = PaymentOrder.builder()
				.bankCode("KTB")
				.localDate(LocalDate.of(2020, Month.JANUARY, 2))
				.value(new BigDecimal("2"))
				.build();
		paymentOrderList.add(paymentOrder);
		paymentOrder = PaymentOrder.builder()
				.bankCode("CIMB")
				.localDate(LocalDate.of(2020, Month.JANUARY, 3))
				.value(new BigDecimal("3"))
				.build();
		paymentOrderList.add(paymentOrder);
		paymentOrder = PaymentOrder.builder()
				.bankCode("KTB")
				.localDate(LocalDate.of(2020, Month.JANUARY, 4))
				.value(new BigDecimal("4"))
				.build();
		paymentOrderList.add(paymentOrder);
		paymentOrder = PaymentOrder.builder()
				.bankCode("CIMB")
				.localDate(LocalDate.of(2020, Month.JANUARY, 5))
				.value(new BigDecimal("5"))
				.build();
		paymentOrder = PaymentOrder.builder()
				.bankCode("SCB")
				.localDate(LocalDate.of(2020, Month.JANUARY, 6))
				.value(new BigDecimal("6"))
				.build();
		paymentOrderList.add(paymentOrder);



		Map<String, List<PaymentOrder>> result = paymentOrderList.stream()
				.collect(Collectors.groupingBy(PaymentOrder::getBankCode));

		result.forEach((key, value) -> {
			log.info(key);
			try {
				for (PaymentOrder order : value) {
					order.getBankCode();
					order.getValue();
				}

				log.info(new ObjectMapper().writeValueAsString(value));
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
		});

	}

}
