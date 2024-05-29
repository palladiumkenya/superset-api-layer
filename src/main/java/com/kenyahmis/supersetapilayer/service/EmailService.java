package com.kenyahmis.supersetapilayer.service;

import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Date;

@Service
public class EmailService {

    private final Session mailSession;
    private final static String REPLY_EMAIL = "no-replymg.kenyahmis.org";
    private final static Logger LOG = LoggerFactory.getLogger(EmailService.class);

    public EmailService(final Session mailSession) {
        this.mailSession = mailSession;
    }

    public void sendEmail(String from, String to, String subject, String text) {
        try
        {
            MimeMessage msg = new MimeMessage(mailSession);
            msg.addHeader("Content-type", "text/HTML; charset=UTF-8");
            msg.addHeader("format", "flowed");
            msg.addHeader("Content-Transfer-Encoding", "8bit");
            msg.setFrom(new InternetAddress(from));
            msg.setReplyTo(InternetAddress.parse(REPLY_EMAIL, false));
            msg.setSubject(subject, "UTF-8");
            msg.setText(text, "UTF-8");
            msg.setSentDate(new Date());
            msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to, false));
            Transport.send(msg);
            LOG.info("Email '{}' sent successfully to {}", subject, to);
        }
        catch (MessagingException e) {
            LOG.error(e.getMessage(), e);
        }
    }
}
