package th.co.solar.solarapi.scheduler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import th.co.solar.solarapi.service.ConsumerService;

import java.io.IOException;
import java.util.Date;

@Slf4j
@Component
public class QueueScheduler {

    @Value("${queue.enable:false}")
    private boolean enable;

    @Autowired
    private ConsumerService consumerService;

    @Scheduled(cron = "${queue.interval-cron}", zone="Asia/Bangkok")
    public void execute() {
        if (enable) {
            try {
                consumerService.processQueueTotal();
            } catch (Exception e) {
                log.error(e.getMessage());
                e.printStackTrace();
            }
        }
    }

    @Scheduled(cron = "${queue.weather-cron}", zone="Asia/Bangkok")
    public void executeWeather() throws IOException {
        if (enable) {
            consumerService.processQueueWeather();
        }
    }

    @Scheduled(cron = "${queue.weather-today-cron}", zone="Asia/Bangkok")
    public void executeWeatherToDay() throws IOException {
        if (enable) {
            consumerService.processQueueWeatherToday();
        }
    }

    @Scheduled(cron = "${queue.weather-3-hours-cron}", zone="Asia/Bangkok")
    public void executeWeather3Hours() throws IOException {
        if (enable) {
            consumerService.processQueueWeather3Hours();
        }
    }
}
