package com.kenyahmis.supersetapilayer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kenyahmis.supersetapilayer.properties.SupersetApiProperties;
import com.kenyahmis.supersetapilayer.properties.OpenmetadataApiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.util.*;

@Service
public class APIService {
    private final RestClient defaultClient;
    private final SupersetApiProperties supersetApiProperties;
    private final OpenmetadataApiProperties openmetadataApiProperties;
    private final static int DEFAULT_PAGE = 0;
    private final static int DEFAULT_PAGE_SIZE = 200;
    private final Logger LOG = LoggerFactory.getLogger(APIService.class);

    public APIService(RestClient defaultClient, SupersetApiProperties supersetApiProperties, OpenmetadataApiProperties openmetadataApiProperties) {
        this.defaultClient = defaultClient;
        this.supersetApiProperties = supersetApiProperties;
        this.openmetadataApiProperties = openmetadataApiProperties;
    }

    private String getAccessToken() {
        Map<String, String> requestBody = new HashMap<>();
        requestBody.put("password", supersetApiProperties.getPassword());
        requestBody.put("provider", supersetApiProperties.getProvider());
        requestBody.put("refresh", supersetApiProperties.getRefresh());
        requestBody.put("username", supersetApiProperties.getUsername());
        String baseUrl = supersetApiProperties.getBaseUrl();

        String uri = String.format("https://%s/api/v1/security/login", baseUrl);
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
        String uri  = String.format("https://%s/api/v1/dataset/?q=(page:%d,page_size:%d,columns:!(%s))", host, DEFAULT_PAGE, DEFAULT_PAGE_SIZE, columns);
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

    private JsonNode getColumnsAll(Integer datasetId){
        String token = getAccessToken();
        final String host = supersetApiProperties.getBaseUrl();
        String uri  = String.format("https://%s/api/v1/dataset/%d", host, datasetId);
        LOG.info("URI is: {}", uri);
        ResponseEntity<JsonNode> response =  defaultClient.get()
                .uri(uri)
                .header("Authorization","Bearer " + token)
                .retrieve()
                .toEntity(JsonNode.class);
        JsonNode dataset = response.getBody();
        assert dataset != null;
        return dataset.get("result");
    }

    private void updateColumnDescriptions(List<JsonNode> columns, Integer datasetId){
        String token = getAccessToken();
        final String host = supersetApiProperties.getBaseUrl();
        String uri  = String.format("https://%s/api/v1/dataset/%d?override_columns=true", host, datasetId);
        JsonNode requestBody = JsonNodeFactory.instance.objectNode()
                .set("columns", JsonNodeFactory.instance.arrayNode()
                        .addAll(columns));
        LOG.info(requestBody.toString());
        LOG.info("URI is: {}", uri);
        ResponseEntity<JsonNode> response =  defaultClient.put()
                .uri(uri)
                .header("Authorization","Bearer " + token)
                .body(requestBody)
                .retrieve()
                .toEntity(JsonNode.class);
    }

    private JsonNode formatColumnNode(JsonNode column, String... keysToRemove) {
        if (column.isObject()) {
            ObjectNode modifiedNode = ((ObjectNode) column).deepCopy();
            for (String key : keysToRemove) {
                modifiedNode.remove(key);
            }
            return modifiedNode;
        }
        return column;
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

    public void populateDescriptions() {
        Iterator<JsonNode> datasets =  getDatasets();
        if (datasets == null) {
            LOG.info("No datasets found");
            return;
        }
        final String omHost = openmetadataApiProperties.getBaseUrl();
        final String jwtToken = openmetadataApiProperties.getJwtToken();
        while (datasets.hasNext()) {
            Integer id = datasets.next().intValue();

            JsonNode datasetInfo = getColumnsAll(id);
            String database = datasetInfo.get("table_name").textValue();
            Iterator<JsonNode> columns = datasetInfo.get("columns").iterator();
            List<JsonNode> newColumns = new ArrayList<>();

            while(columns.hasNext()){
                JsonNode column = columns.next();
                String glossaryUri = String.format("https://%s/api/v1/glossaryTerms/name/National Datawarehouse Data Dictionary.%s.%s", omHost, database, column.get("column_name").textValue());
                LOG.info("Accessing glossary URI: {}", glossaryUri);
                try {
                    ResponseEntity<JsonNode> response =  defaultClient.get()
                            .uri(glossaryUri)
                            .header("Authorization","Bearer " + jwtToken)
                            .retrieve()
                            .toEntity(JsonNode.class);

                    if(response.getStatusCode().is2xxSuccessful()) {
                        JsonNode dataset = response.getBody();
                        assert dataset != null;
                        ((ObjectNode)column).put("description", dataset.get("description"));
                        // Remove unwanted columns
                        newColumns.add(formatColumnNode(column, "changed_on", "created_on", "type_generic", "python_date_format"));
                        LOG.info("Updated Column {}.{}", database, column.get("column_name").textValue());
                    }
                } catch (HttpClientErrorException he) {
                    if (he.getStatusCode().is4xxClientError()) {
                        // Log a message for 404 and skip the loop iteration
                        LOG.warn("Glossary term not found for URI: {}", glossaryUri);
                    } else {
                        LOG.error("Failed to updated dataset with message {}", he.getResponseBodyAs(String.class), he);
                    }
                }
            }
            // update the column definitions
            updateColumnDescriptions(newColumns, id);
        }
    }
}
