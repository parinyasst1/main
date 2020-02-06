package th.co.solar.solarapi.scheduler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import th.co.solar.solarapi.service.ConsumerDataService;
import th.co.solar.solarapi.service.ConsumerService;
import th.co.solar.solarapi.service.RemoveDataService;

import java.io.IOException;
import java.util.Date;

@Slf4j
@Component
public class QueueScheduler {

    @Value("${queue.enable:false}")
    private boolean enable;

    @Autowired
    private ConsumerService consumerService;
    @Autowired
    private RemoveDataService removeDataService;
    @Autowired
    private ConsumerDataService consumerDataService;

    @Scheduled(cron = "${queue.interval-cron}", zone="Asia/Bangkok")
    public void execute() {
        if (enable) {
            try {
                consumerDataService.processQueueTotal();
            } catch (Exception e) {
                log.error(e.getMessage());
                e.printStackTrace();
            }
        }
    }

    @Scheduled(cron = "${queue.remove-cron}", zone="Asia/Bangkok")
    public void remove() {
        if (enable) {
            try {
                removeDataService.processQueue();
            } catch (Exception e) {
                log.error(e.getMessage());
                e.printStackTrace();
            }
        }
    }

    @Scheduled(cron = "${queue.weather-7day-cron}", zone="Asia/Bangkok")
    public void executeWeather() throws IOException {
        if (enable) {
            consumerService.processQueueWeatherForecast7Days();
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
