package ma.enset.web_producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.UUID;

// @RestController tells Spring: "I am going to handle incoming web traffic"
@RestController
@RequestMapping("/api")
public class ClickController {

    // KafkaTemplate is Spring's built-in tool for sending messages to a broker
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    // @PostMapping catches the exact request sent by the JavaScript fetch() function
    @PostMapping("/click")
    public ResponseEntity<String> registerClick() {

        // 1. Generate a fake user ID for whoever clicked the button (e.g., "user-9f8a2")
        String userId = "user-" + UUID.randomUUID().toString().substring(0, 5);

        // 2. Use the KafkaTemplate to send the data to the "clicks" topic
        kafkaTemplate.send("clicks", userId, "click");

        // 3. Tell the web browser "OK, I got it!"
        return ResponseEntity.ok("Event published to Kafka!");
    }
}