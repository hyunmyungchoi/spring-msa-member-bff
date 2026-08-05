package com.springmsa.memberbff.chat.kafka;

import com.springmsa.kafka.event.ChatMessageCreatedEvent;
import com.springmsa.kafka.topic.MsaKafkaTopics;
import com.springmsa.memberbff.chat.dto.ChatMessageResponse;
import com.springmsa.memberbff.chat.event.ChatMessageSavedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

@Component
@ConditionalOnProperty(prefix = "app.kafka", name = "enabled", havingValue = "true")
@RequiredArgsConstructor
public class ChatMessageKafkaEventPublisher {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    @TransactionalEventListener(phase = TransactionPhase.BEFORE_COMMIT)
    public void publish(ChatMessageSavedEvent event) {
        ChatMessageCreatedEvent payload = toPayload(event.message());
        String serializedPayload = serialize(payload);
        jdbcTemplate.update("""
                        INSERT INTO member_bff.outbox_events (
                            event_id, aggregate_type, aggregate_id, event_type, topic, event_key,
                            payload, occurred_at, attempts
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                        """,
                UUID.randomUUID(), "ChatMessage", event.message().streamId(), "chat.message-created",
                MsaKafkaTopics.CHAT_MESSAGE_CREATED, payload.roomId(), serializedPayload,
                payload.occurredAt().atOffset(ZoneOffset.UTC));
    }

    private ChatMessageCreatedEvent toPayload(ChatMessageResponse message) {
        Long messageId = parseMessageId(message.streamId());

        return new ChatMessageCreatedEvent(
                "chat-message-created:" + message.streamId(),
                messageId,
                message.roomId(),
                message.senderUserId(),
                message.senderLoginId(),
                message.senderName(),
                message.content(),
                message.sentAt(),
                Instant.now()
        );
    }

    private Long parseMessageId(String value) {
        try {
            return Long.parseLong(value);

        } catch (NumberFormatException e) {
            return null;
        }
    }

    private String serialize(ChatMessageCreatedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);

        } catch (JacksonException e) {
            throw new IllegalStateException("Failed to serialize chat message event", e);
        }
    }
}
