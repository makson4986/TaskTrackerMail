package org.makson.tasktrackermail.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.makson.tasktrackermail.dto.MailDto;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mail.MailException;
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

    @KafkaListener(topics = "${spring.kafka.topics.report}")
    private void sendTaskReport(MailDto report) {
        log.info("Received report for {}", report.recipientEmail());
        sendMail(report.recipientEmail(), report.title(), report.content());
        log.info("Daily task report for user {} has been sent", report.recipientEmail());
    }

    @KafkaListener(topics = "${spring.kafka.topics.greeting}")
    private void sendGreeting(MailDto greeting) {
        log.info("Received greeting for {}", greeting.recipientEmail());
        sendMail(greeting.recipientEmail(), greeting.title(), greeting.content());
        log.info("A welcome email has been sent to user {}", greeting.recipientEmail());
    }

    private void sendMail(String to, String subject, String text) {
        SimpleMailMessage mail = new SimpleMailMessage();

        mail.setFrom(from);
        mail.setTo(to);
        mail.setSubject(subject);
        mail.setText(text);

        try {
            mailSender.send(mail);
        } catch (MailException e) {
            throw new RuntimeException(e);
        }
    }
}
