package com.kenyahmis.supersetapilayer;

import com.kenyahmis.supersetapilayer.properties.OpenmetadataApiProperties;
import com.kenyahmis.supersetapilayer.properties.ReportingDatabaseProperties;
import com.kenyahmis.supersetapilayer.properties.SupersetApiProperties;
import com.kenyahmis.supersetapilayer.properties.SupersetDatabaseProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.mail.MailProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestClient;

@EnableConfigurationProperties({SupersetApiProperties.class, OpenmetadataApiProperties.class,
		SupersetDatabaseProperties.class, ReportingDatabaseProperties.class, MailProperties.class})
@SpringBootApplication
public class SupersetApiLayerApplication {

	public static void main(String[] args) {
		SpringApplication.run(SupersetApiLayerApplication.class, args);
	}

	@Bean
	RestClient defaultClient() {
		return RestClient.create();
	}
}
