package th.co.solar.solarapi.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import th.co.solar.solarapi.service.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/")
@Slf4j
@CrossOrigin(origins = "*")
public class ProcessController {

    @Autowired
    ConsumerService consumerService;
    @Autowired
    ConsumerDataService consumerDataService;
    @Autowired
    RemoveDataService removeDataService;
    @Autowired
    ParameterDailyService parameterDailyService;
    @Autowired
    ParameterMonthlyService parameterMonthlyService;
    @Autowired
    ParameterYearlyService parameterYearlyService;

    @GetMapping(value = "/removeData", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> removeDataService() {
        return ResponseEntity.ok(removeDataService.processQueue());
    }

    @GetMapping(value = "/sumTotal", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> consumerData() {
        return ResponseEntity.ok(consumerDataService.processQueueTotal());
    }

    @GetMapping(value = "/sumDailyTotal", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> consumerDataDaily() {
        return ResponseEntity.ok(parameterDailyService.processQueueTotal());
    }

    @GetMapping(value = "/sumMonthlyTotal", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> consumerDataMonthly() {
        return ResponseEntity.ok(parameterMonthlyService.processQueueTotal());
    }

    @GetMapping(value = "/sumYearlyTotal", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> consumerDataYearly() {
        return ResponseEntity.ok(parameterYearlyService.processQueueTotal());
    }

    @GetMapping(value = "/weatherToday", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> processQueueWeatherToday() {
        consumerService.processQueueWeatherToday();
        return ResponseEntity.ok("OK");
    }

    @GetMapping(value = "/weather3Hours", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> processQueueWeather3Hours() {
        consumerService.processQueueWeather3Hours();
        return ResponseEntity.ok("OK");
    }

    @GetMapping(value = "/weatherForecast7Days", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> processQueueWeatherForecast7Days() {
        consumerService.processQueueWeatherForecast7Days();
        return ResponseEntity.ok("OK");
    }

}

