package io.github.fernandolopes.test;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SinkRecordTest {

	private static final String TOPIC_NAME = "myTopic";
	private static final Integer PARTITION_NUMBER = 0;
	private static final long KAFKA_OFFSET = 0L;
	private static final Long KAFKA_TIMESTAMP = 0L;
	private static final TimestampType TS_TYPE = TimestampType.CREATE_TIME;
	
	private SinkRecord record;
	
	@BeforeEach
	public void beforeEach() {
	    record = new SinkRecord(TOPIC_NAME, PARTITION_NUMBER, Schema.STRING_SCHEMA, "key", Schema.BOOLEAN_SCHEMA, false, KAFKA_OFFSET,
	                            KAFKA_TIMESTAMP, TS_TYPE, null, TOPIC_NAME, PARTITION_NUMBER, KAFKA_OFFSET);
	}
	    
	@Test
    public void shouldCreateSinkRecordWithHeaders() {
        Headers headers = new ConnectHeaders().addString("h1", "hv1").addBoolean("h2", true);
        record = new SinkRecord(TOPIC_NAME, PARTITION_NUMBER, Schema.STRING_SCHEMA, "key", Schema.BOOLEAN_SCHEMA, false, KAFKA_OFFSET,
                                KAFKA_TIMESTAMP, TS_TYPE, headers);
        assertNotNull(record.headers());
        assertSame(headers, record.headers());
        assertFalse(record.headers().isEmpty());
    }
	
	@Test
    public void shouldCreateSinkRecordWithEmptyHeaders() {
        assertEquals(TOPIC_NAME, record.topic());
        assertEquals(PARTITION_NUMBER, record.kafkaPartition());
        assertEquals(Schema.STRING_SCHEMA, record.keySchema());
        assertEquals("key", record.key());
        assertEquals(Schema.BOOLEAN_SCHEMA, record.valueSchema());
        assertEquals(false, record.value());
        assertEquals(KAFKA_OFFSET, record.kafkaOffset());
        assertEquals(KAFKA_TIMESTAMP, record.timestamp());
        assertEquals(TS_TYPE, record.timestampType());
        assertNotNull(record.headers());
        assertTrue(record.headers().isEmpty());
    }

    @Test
    public void shouldDuplicateRecordAndCloneHeaders() {
        SinkRecord duplicate = record.newRecord(TOPIC_NAME, PARTITION_NUMBER, Schema.STRING_SCHEMA, "key", Schema.BOOLEAN_SCHEMA, false,
                                                KAFKA_TIMESTAMP);

        assertEquals(TOPIC_NAME, duplicate.topic());
        assertEquals(PARTITION_NUMBER, duplicate.kafkaPartition());
        assertEquals(Schema.STRING_SCHEMA, duplicate.keySchema());
        assertEquals("key", duplicate.key());
        assertEquals(Schema.BOOLEAN_SCHEMA, duplicate.valueSchema());
        assertEquals(false, duplicate.value());
        assertEquals(KAFKA_OFFSET, duplicate.kafkaOffset());
        assertEquals(KAFKA_TIMESTAMP, duplicate.timestamp());
        assertEquals(TS_TYPE, duplicate.timestampType());
        assertNotNull(duplicate.headers());
        assertTrue(duplicate.headers().isEmpty());
        assertNotSame(record.headers(), duplicate.headers());
        assertEquals(record.headers(), duplicate.headers());
    }


    @Test
    public void shouldDuplicateRecordUsingNewHeaders() {
        Headers newHeaders = new ConnectHeaders().addString("h3", "hv3");
        SinkRecord duplicate = record.newRecord(TOPIC_NAME, PARTITION_NUMBER, Schema.STRING_SCHEMA, "key", Schema.BOOLEAN_SCHEMA, false,
                                                KAFKA_TIMESTAMP, newHeaders);

        assertEquals(TOPIC_NAME, duplicate.topic());
        assertEquals(PARTITION_NUMBER, duplicate.kafkaPartition());
        assertEquals(Schema.STRING_SCHEMA, duplicate.keySchema());
        assertEquals("key", duplicate.key());
        assertEquals(Schema.BOOLEAN_SCHEMA, duplicate.valueSchema());
        assertEquals(false, duplicate.value());
        assertEquals(KAFKA_OFFSET, duplicate.kafkaOffset());
        assertEquals(KAFKA_TIMESTAMP, duplicate.timestamp());
        assertEquals(TS_TYPE, duplicate.timestampType());
        assertNotNull(duplicate.headers());
        assertEquals(newHeaders, duplicate.headers());
        assertSame(newHeaders, duplicate.headers());
        assertNotSame(record.headers(), duplicate.headers());
        assertNotEquals(record.headers(), duplicate.headers());
    }

    @Test
    public void shouldModifyRecordHeader() {
        assertTrue(record.headers().isEmpty());
        record.headers().addInt("intHeader", 100);
        assertEquals(1, record.headers().size());
        Header header = record.headers().lastWithName("intHeader");
        assertEquals(100, (int) Values.convertToInteger(header.schema(), header.value()));
    }

    @Test
    public void shouldRetainOriginalTopicPartition() {
        SinkRecord transformed = record.newRecord("transformed-topic", PARTITION_NUMBER + 1, Schema.STRING_SCHEMA, "key",
                Schema.BOOLEAN_SCHEMA, false, KAFKA_TIMESTAMP);

        assertEquals(TOPIC_NAME, transformed.originalTopic());
        assertEquals(PARTITION_NUMBER, transformed.originalKafkaPartition());

        SinkRecord transformed2 = transformed.newRecord("transformed-topic-2", PARTITION_NUMBER + 2, Schema.STRING_SCHEMA, "key",
                Schema.BOOLEAN_SCHEMA, false, KAFKA_TIMESTAMP);

        assertEquals(TOPIC_NAME, transformed2.originalTopic());
        assertEquals(PARTITION_NUMBER, transformed2.originalKafkaPartition());
    }

    @Test
    public void shouldRetainOriginalTopicPartitionWithOlderConstructor() {
        SinkRecord record = new SinkRecord(TOPIC_NAME, PARTITION_NUMBER, Schema.STRING_SCHEMA, "key", Schema.BOOLEAN_SCHEMA,
                false, KAFKA_OFFSET, KAFKA_TIMESTAMP, TS_TYPE, null);
        SinkRecord transformed = record.newRecord("transformed-topic", PARTITION_NUMBER + 1, Schema.STRING_SCHEMA, "key",
                Schema.BOOLEAN_SCHEMA, false, KAFKA_TIMESTAMP);

        assertEquals(TOPIC_NAME, transformed.originalTopic());
        assertEquals(PARTITION_NUMBER, transformed.originalKafkaPartition());

        SinkRecord transformed2 = transformed.newRecord("transformed-topic-2", PARTITION_NUMBER + 2, Schema.STRING_SCHEMA, "key",
                Schema.BOOLEAN_SCHEMA, false, KAFKA_TIMESTAMP);

        assertEquals(TOPIC_NAME, transformed2.originalTopic());
        assertEquals(PARTITION_NUMBER, transformed2.originalKafkaPartition());
    }
}
