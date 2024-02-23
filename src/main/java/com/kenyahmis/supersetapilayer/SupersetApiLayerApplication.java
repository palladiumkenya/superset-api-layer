package com.kenyahmis.supersetapilayer;

import com.kenyahmis.supersetapilayer.properties.OpenmetadataApiProperties;
import com.kenyahmis.supersetapilayer.properties.SupersetApiProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestClient;

@EnableConfigurationProperties({SupersetApiProperties.class, OpenmetadataApiProperties.class})
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
