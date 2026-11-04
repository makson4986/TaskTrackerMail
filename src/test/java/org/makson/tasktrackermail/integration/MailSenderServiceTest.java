package org.makson.tasktrackermail.integration;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.makson.tasktrackermail.config.TestcontainersConfig;
import org.makson.tasktrackermail.dto.MailDto;
import org.makson.tasktrackermail.services.MailSenderService;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.concurrent.TimeUnit;

@SpringBootTest
@ActiveProfiles("test")
@Import(TestcontainersConfig.class)
public class MailSenderServiceTest {
    @MockitoBean
    JavaMailSender mailSender;
    final MailSenderService mailSenderService;
    final KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    public MailSenderServiceTest(MailSenderService mailSenderService, KafkaTemplate<String, Object> kafkaTemplate) {
        this.mailSenderService = mailSenderService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Test
    @DisplayName("A greeting received from a Kafka topic must be successfully sent to the user's email")
    void shouldGreetingSentViaMail(@Value("${spring.kafka.topics.greeting}") String greetingTopic) {
        MailDto mail = new MailDto("test@test.test", "title", "content");
        ArgumentCaptor<SimpleMailMessage> captor = ArgumentCaptor.forClass(SimpleMailMessage.class);

        kafkaTemplate.send(greetingTopic, mail.recipientEmail(), mail);

        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    Mockito.verify(mailSender).send(captor.capture());

                    SimpleMailMessage mailMessage = captor.getValue();

                    Assertions.assertNotNull(mailMessage.getTo());
                    Assertions.assertEquals(mail.recipientEmail(), mailMessage.getTo()[0]);
                });
    }
}
