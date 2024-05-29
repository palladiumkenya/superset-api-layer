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

    @Bean
    public Authenticator mailAuthenticator(final MailProperties mailProperties) {
        return new Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(mailProperties.getUsername(), mailProperties.getPassword());
            }
        };
    }

    @Bean
    public Session mailSession(final MailProperties mailProperties, final Authenticator mailAuthenticator) {
        Properties props = new Properties();
        props.put("mail.smtp.auth", mailProperties.getProperties().get("mail.smtp.auth"));
        props.put("mail.smtp.starttls.enable", mailProperties.getProperties().get("mail.smtp.starttls.enable"));
        props.put("mail.smtp.host", mailProperties.getHost());
        props.put("mail.smtp.port", mailProperties.getPort());
        return Session.getInstance(props, mailAuthenticator);
    }
}
