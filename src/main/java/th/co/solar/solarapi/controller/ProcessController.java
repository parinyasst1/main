package th.co.solar.solarapi.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import th.co.solar.solarapi.service.ConsumerDataService;
import th.co.solar.solarapi.service.ConsumerService;
import th.co.solar.solarapi.service.RemoveDataService;

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

    @GetMapping(value = "/removeData", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> removeDataService() {
        return ResponseEntity.ok(removeDataService.processQueue());
    }

    @GetMapping(value = "/sumTotal", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> consumerData() {
        return ResponseEntity.ok(consumerDataService.processQueueTotal());
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

}

