package com.kenyahmis.supersetapilayer.controller;

import com.kenyahmis.supersetapilayer.service.APIService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class APIController {
    private final APIService apiService;

    public APIController(APIService apiService) {
        this.apiService = apiService;
    }

    @PutMapping(path = "/dataset/refresh")
    private ResponseEntity<String> refreshDatasets() {
        apiService.refreshDatasets();
        return new ResponseEntity<>("Datasets refreshed", HttpStatus.OK);
    }

    @PutMapping(path = "/dataset/description")
    private ResponseEntity<String> updateDescriptions() {
        apiService.populateDescriptions();
        return new ResponseEntity<>("Dataset Descriptions added", HttpStatus.OK);
    }

    @PutMapping(path = "/dataset/sync")
    private ResponseEntity<String> syncDatasets() {
        apiService.addNewDatasets();
        return new ResponseEntity<>("New datasets synced", HttpStatus.OK);
    }
    @GetMapping(path = "/dataset/changelog")
    private ResponseEntity<String> generateChangeLog() {
            apiService.generateAndShareChangeLog();
            return new ResponseEntity<>("Dataset changelog generated", HttpStatus.OK);
    }
}
