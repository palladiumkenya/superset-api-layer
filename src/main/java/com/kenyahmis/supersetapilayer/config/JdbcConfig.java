package com.kenyahmis.supersetapilayer.config;

import com.kenyahmis.supersetapilayer.properties.ReportingDatabaseProperties;
import com.kenyahmis.supersetapilayer.properties.SupersetDatabaseProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

@Configuration
public class JdbcConfig {

    @Bean
    public DataSource mssqlDatasource(ReportingDatabaseProperties reportingDatabaseProperties) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        dataSource.setUrl(String.format("jdbc:sqlserver://%s;encrypt=false;databaseName=%s",
                reportingDatabaseProperties.getHost(), reportingDatabaseProperties.getDatabase()));
        dataSource.setUsername(reportingDatabaseProperties.getUsername());
        dataSource.setPassword(reportingDatabaseProperties.getPassword());
        return dataSource;
    }

    @Bean
    public DataSource postgresDatasource(SupersetDatabaseProperties supersetDatabaseProperties) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl(String.format("jdbc:postgresql://%s:3306/%s", supersetDatabaseProperties.getHost(),
                supersetDatabaseProperties.getDatabase()));
        dataSource.setUsername(supersetDatabaseProperties.getUsername());
        dataSource.setPassword(supersetDatabaseProperties.getPassword());
        return dataSource;
    }

    @Bean
    public JdbcTemplate mssqlJdbcTemplate(DataSource mssqlDatasource) {
        return new JdbcTemplate(mssqlDatasource);
    }

    @Bean
    public JdbcTemplate postgresJdbcTemplate(DataSource postgresDatasource) {
        return new JdbcTemplate(postgresDatasource);
    }
}
