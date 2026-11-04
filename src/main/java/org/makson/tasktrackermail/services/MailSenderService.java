package org.makson.tasktrackermail.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.makson.tasktrackermail.dto.MailDto;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class MailSenderService {
    @Value("${spring.mail.from}")
    private String from;
    private final JavaMailSender mailSender;

    @KafkaListener(topics = {
            "${spring.kafka.topics.report}",
            "${spring.kafka.topics.greeting}"
    })
    public void handleMailCreatedEvent(MailDto report) {
        try {
            log.info("Greeting successfully received for {}", report.recipientEmail());
            sendMail(report.recipientEmail(), report.title(), report.content());
            log.info("Email for user {} has been sent", report.recipientEmail());
        } catch (Exception e) {
            log.error("Error: ", e);
            throw new RuntimeException(e);
        }
    }

    private void sendMail(String to, String subject, String text) {
        SimpleMailMessage mail = new SimpleMailMessage();

        mail.setFrom(from);
        mail.setTo(to);
        mail.setSubject(subject);
        mail.setText(text);

        try {
            mailSender.send(mail);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
