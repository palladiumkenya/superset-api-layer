package com.kenyahmis.supersetapilayer.controller;

import com.kenyahmis.supersetapilayer.service.APIService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class APIController {
    private final APIService apiService;

    public APIController(APIService apiService) {
        this.apiService = apiService;
    }

    @PatchMapping(path = "/datasets/refresh")
    private ResponseEntity<String> testAccessToken() {
        apiService.refreshDatasets();
        return new ResponseEntity<>("Datasets refreshed", HttpStatus.OK);
    }

    @GetMapping(path = "/datasets/description")
    private ResponseEntity<String> testAccess() {
        apiService.populateDescriptions();
        return new ResponseEntity<>("Dataset Descriptions added", HttpStatus.OK);
    }
}
