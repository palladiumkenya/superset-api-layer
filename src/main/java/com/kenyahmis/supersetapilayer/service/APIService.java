package com.kenyahmis.supersetapilayer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.kenyahmis.supersetapilayer.properties.SupersetApiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Service
public class APIService {
    private final RestClient defaultClient;
    private final SupersetApiProperties supersetApiProperties;
    private final static int DEFAULT_PAGE = 0;
    private final static int DEFAULT_PAGE_SIZE = 200;
    private final Logger LOG = LoggerFactory.getLogger(APIService.class);

    public APIService(RestClient defaultClient, SupersetApiProperties supersetApiProperties) {
        this.defaultClient = defaultClient;
        this.supersetApiProperties = supersetApiProperties;
    }

    private String getAccessToken() {
        Map<String, String> requestBody = new HashMap<>();
        requestBody.put("password", supersetApiProperties.getPassword());
        requestBody.put("provider", supersetApiProperties.getProvider());
        requestBody.put("refresh", supersetApiProperties.getRefresh());
        requestBody.put("username", supersetApiProperties.getUsername());
        String baseUrl = supersetApiProperties.getBaseUrl();

        String uri = String.format("http://%s/api/v1/security/login", baseUrl);
        ResponseEntity<JsonNode> response =  defaultClient.post()
                .uri(uri)
                .contentType(MediaType.APPLICATION_JSON)
                .body(requestBody)
                .retrieve()
                .toEntity(JsonNode.class);
        JsonNode node = response.getBody();
        String token = null;
        if (node != null) {
            token = node.get("access_token").textValue();
           LOG.info(node.toString());
        }
        return token;
    }

    private Iterator<JsonNode> getDatasets(){
        String token = getAccessToken();
        final String host = supersetApiProperties.getBaseUrl();
        final String columns = "id";
        String uri  = String.format("http://%s/api/v1/dataset/?q=(page:%d,page_size:%d,columns:!(%s))", host, DEFAULT_PAGE, DEFAULT_PAGE_SIZE, columns);
        LOG.info("URI is: {}", uri);
        ResponseEntity<JsonNode> response =  defaultClient.get()
                .uri(uri)
                .header("Authorization","Bearer " + token)
                .retrieve()
                .toEntity(JsonNode.class);
        JsonNode node = response.getBody();
        Iterator<JsonNode> datasets = null;
        if (node != null) {
            datasets = node.get("ids").iterator();
        }
        return datasets;
    }

    public void refreshDatasets() {
        Iterator<JsonNode> datasets =  getDatasets();
        if (datasets == null) {
            LOG.info("No datasets found");
            return;
        }
        final String host = supersetApiProperties.getBaseUrl();
        final String accessToken = getAccessToken();
        while (datasets.hasNext()) {
            Integer id = datasets.next().intValue();
            if (id > 0) {
                String uri  = String.format("http://%s/api/v1/dataset/%d/refresh", host, id);
                try {
                    ResponseEntity<JsonNode> response =  defaultClient.put()
                            .uri(uri)
                            .header("Authorization","Bearer " + accessToken)
                            .retrieve()
                            .toEntity(JsonNode.class);
                    if(response.getStatusCode().is2xxSuccessful()) {
                        LOG.info("Refreshed datasets {}", id);
                    }
                } catch (HttpClientErrorException he) {
                    he.getResponseBodyAs(String.class);
                    LOG.error("Failed to updated dataset {} with message {}", id, he.getResponseBodyAs(String.class), he );
                }
            }
        }
    }
}
