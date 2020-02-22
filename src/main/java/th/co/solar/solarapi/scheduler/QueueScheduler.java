package th.co.solar.solarapi.scheduler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import th.co.solar.solarapi.service.*;

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
    @Autowired
    private ParameterDailyService parameterDailyService;
    @Autowired
    private ParameterMonthlyService parameterMonthlyService;
    @Autowired
    private ParameterYearlyService parameterYearlyService;

    @Scheduled(cron = "${queue.daily-total-cron}", zone="Asia/Bangkok")
    public void dailyTotal() {
        if (enable) {
            try {
                parameterDailyService.processQueueTotal();
            } catch (Exception e) {
                log.error(e.getMessage());
                e.printStackTrace();
            }
        }
    }

    @Scheduled(cron = "${queue.monthly-total-cron}", zone="Asia/Bangkok")
    public void monthlyTotal() {
        if (enable) {
            try {
                parameterMonthlyService.processQueueTotal();
            } catch (Exception e) {
                log.error(e.getMessage());
                e.printStackTrace();
            }
        }
    }

    @Scheduled(cron = "${queue.yearly-total-cron}", zone="Asia/Bangkok")
    public void yearlyTotal() {
        if (enable) {
            try {
                parameterYearlyService.processQueueTotal();
            } catch (Exception e) {
                log.error(e.getMessage());
                e.printStackTrace();
            }
        }
    }

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
