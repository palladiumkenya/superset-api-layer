package com.kenyahmis.supersetapilayer.config;

import jakarta.mail.Authenticator;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import org.springframework.boot.autoconfigure.mail.MailProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.Properties;

@Configuration
public class EmailConfig {

    private Authenticator mailAuthenticator(final MailProperties mailProperties) {
        return new Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(mailProperties.getUsername(), mailProperties.getPassword());
            }
        };
    }

    @Bean
    public Session mailSession(final MailProperties mailProperties) {
        Properties props = new Properties();
        System.out.println(mailProperties.getProperties().keySet());
        props.put("mail.smtp.auth", mailProperties.getProperties().get("auth"));
        props.put("mail.smtp.starttls.enable", mailProperties.getProperties().get("starttls"));
        props.put("mail.smtp.host", mailProperties.getHost());
        props.put("mail.smtp.port", mailProperties.getPort());
        return Session.getInstance(props, mailAuthenticator(mailProperties));
    }
}
