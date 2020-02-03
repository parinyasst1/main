package th.co.solar.solarapi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class SolarapiApplication {

	public static void main(String[] args) {
		SpringApplication.run(SolarapiApplication.class, args);
	}

}
