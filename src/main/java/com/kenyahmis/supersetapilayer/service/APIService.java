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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.util.*;

@Service
public class APIService {
    private final RestClient defaultClient;
    private final SupersetApiProperties supersetApiProperties;
    private final OpenmetadataApiProperties openmetadataApiProperties;
    private final JdbcTemplate mssqlJdbcTemplate;
    private final static int DEFAULT_PAGE = 0;
    private final static int DEFAULT_PAGE_SIZE = 200;
    private final Logger LOG = LoggerFactory.getLogger(APIService.class);

    public APIService(RestClient defaultClient, SupersetApiProperties supersetApiProperties, OpenmetadataApiProperties openmetadataApiProperties,
                      JdbcTemplate mssqlJdbcTemplate) {
        this.defaultClient = defaultClient;
        this.supersetApiProperties = supersetApiProperties;
        this.openmetadataApiProperties = openmetadataApiProperties;
        this.mssqlJdbcTemplate = mssqlJdbcTemplate;
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

    public List<String> getTargetSymmetricDifference(List<String> source, List<String> target) {
        List<String> newDatasets = source.stream().filter(element -> !target.contains(element)).toList();
        return newDatasets;
    }
    public void addNewDatasets() {
        List<String> exclusions = List.of("QueryBuilders", "QueryTransformers", "sysdiagrams");
        List<String> newDatasets = getTargetSymmetricDifference(getReportingDbTableNames(), getDatasetNames())
                .stream().filter(e -> !exclusions.contains(e)).toList();
        LOG.info("Found {} new datasets", newDatasets.size());
        newDatasets.forEach(System.out::println);
        newDatasets.forEach(this::addDataset);
        // TODO implement RLS
    }
    public List<String> getReportingDbTableNames() {
        String fetchReportingTablesQuery = """
                USE REPORTING;
                BEGIN
                SELECT [name]
                FROM sys.tables;
                END
                """;
        List<String> tablesList = null;
        try {
            tablesList = mssqlJdbcTemplate.queryForList(fetchReportingTablesQuery, String.class);
        } catch (Exception e) {
            LOG.error("Failed to fetch reporting DB tables" , e);
        }
        if (tablesList != null) {
            LOG.info("Fetched {} tables from reporting database", tablesList.size());
            tablesList.forEach(System.out::println);
        }

        return tablesList;
    }
    public List<String> getDatasetNames() {
        List<String> datasetNames = new ArrayList<>();
        String token = getAccessToken();
        final String host = supersetApiProperties.getBaseUrl();
        final String columns = "table_name";
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
            datasets = node.get("result").iterator();
            while (datasets.hasNext()) {
                JsonNode tableNameNode = datasets.next();
                datasetNames.add(tableNameNode.get("table_name").textValue());
            }
        }
        datasetNames.forEach(System.out::println);
        return datasetNames;
    }
    private Iterator<JsonNode> getDatasetIds(){
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
    private void addDataset(String datasetName) {
        String token = getAccessToken();
        final String host = supersetApiProperties.getBaseUrl();
        String uri  = String.format("http://%s/api/v1/dataset", host);
        LOG.info("Add dataset URI: {}", uri);
        int reportingDbId = 2;
        int adminId = 1;
        JsonNode requestBody = JsonNodeFactory.instance.objectNode()
                .put("database", reportingDbId)
                .put("is_managed_externally", true)
                .put("schema", "dbo")
                .put("table_name", datasetName)
                .set("owners", JsonNodeFactory.instance.arrayNode()
                        .add(JsonNodeFactory.instance.numberNode(adminId)));
        LOG.info("Add dataset request: {}", requestBody.toString());
        try {
//            ResponseEntity<JsonNode> response =
            ResponseEntity<String> response = defaultClient
                            .post()
                            .uri(uri)
                            .header("Authorization","Bearer " + token)
                            .contentType(MediaType.APPLICATION_JSON)
                    .body(requestBody)
                    .retrieve()
                            .toEntity(String.class);
//                    .toString();
                    LOG.info("Add dataset response: {}", response.toString());
//                    .toEntity(JsonNode.class);
        } catch (HttpClientErrorException ce) {
            LOG.error("Failed to add dataset {} with message {}", datasetName, ce.getResponseBodyAs(String.class), ce );
        }
    }
    private JsonNode getColumnsAll(Integer datasetId){
        String token = getAccessToken();
        final String host = supersetApiProperties.getBaseUrl();
        //TODO: Only select columns that are needed
        String uri  = String.format("http://%s/api/v1/dataset/%d", host, datasetId);
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


    private void updateColumnDescriptions(List<JsonNode> columns, String tableDescription, Integer datasetId){
        String token = getAccessToken();
        final String host = supersetApiProperties.getBaseUrl();
        String uri  = String.format("http://%s/api/v1/dataset/%d?override_columns=true", host, datasetId);
        JsonNode requestBody = JsonNodeFactory.instance.objectNode()
                .put("description", tableDescription)
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
        Iterator<JsonNode> datasets =  getDatasetIds();
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
        Iterator<JsonNode> datasets =  getDatasetIds();
        if (datasets == null) {
            LOG.info("No datasets found");
            return;
        }
        final String omHost = openmetadataApiProperties.getBaseUrl();
        final String jwtToken = openmetadataApiProperties.getJwtToken();
        while (datasets.hasNext()) {
            Integer id = datasets.next().intValue();

            JsonNode datasetInfo = getColumnsAll(id);
            String tableName = datasetInfo.get("table_name").textValue();
            Iterator<JsonNode> columns = datasetInfo.get("columns").iterator();
            List<JsonNode> newColumns = new ArrayList<>();
            String tableDescription = null;
            String tableGlossaryUri = String.format("https://%s/api/v1/glossaryTerms/name/National Datawarehouse Data Dictionary.%s", omHost, tableName);
            try {
                ResponseEntity<JsonNode> response =  defaultClient.get()
                        .uri(tableGlossaryUri)
                        .header("Authorization","Bearer " + jwtToken)
                        .retrieve()
                        .toEntity(JsonNode.class);

                if(response.getStatusCode().is2xxSuccessful()) {
                    JsonNode glossaryTerm = response.getBody();
                    assert glossaryTerm != null;
                    tableDescription = glossaryTerm.get("description").textValue();
                }
            } catch (HttpClientErrorException he) {
                if (he.getStatusCode().is4xxClientError()) {
                    // Log a message for 404 and skip the loop iteration
                    LOG.warn("Glossary term not found for URI: {}", tableGlossaryUri);
                } else {
                    LOG.error("Failed to updated dataset with message {}", he.getResponseBodyAs(String.class), he);
                }
            }

            if (tableDescription != null) {
                while (columns.hasNext()) {
                    JsonNode column = columns.next();
                    String columnGlossaryUri = String.format("https://%s/api/v1/glossaryTerms/name/National Datawarehouse Data Dictionary.%s.%s", omHost, tableName, column.get("column_name").textValue());
                    LOG.info("Accessing glossary URI: {}", columnGlossaryUri);
                    try {
                        ResponseEntity<JsonNode> response = defaultClient.get()
                                .uri(columnGlossaryUri)
                                .header("Authorization", "Bearer " + jwtToken)
                                .retrieve()
                                .toEntity(JsonNode.class);

                        if (response.getStatusCode().is2xxSuccessful()) {
                            JsonNode dataset = response.getBody();
                            assert dataset != null;
                            ((ObjectNode) column).put("description", dataset.get("description"));
                            // Remove unwanted columns
                            newColumns.add(formatColumnNode(column, "changed_on", "created_on", "type_generic", "python_date_format"));
                            LOG.info("Updated Column {}.{}", tableName, column.get("column_name").textValue());
                        }
                    } catch (HttpClientErrorException he) {
                        if (he.getStatusCode().is4xxClientError()) {
                            // Log a message for 404 and skip the loop iteration
                            LOG.warn("Glossary term not found for URI: {}", columnGlossaryUri);
                        } else {
                            LOG.error("Failed to updated dataset with message {}", he.getResponseBodyAs(String.class), he);
                        }
                    }
                }
                // update the table & column definitions
                updateColumnDescriptions(newColumns, tableDescription, id);
            }
        }
    }
}
