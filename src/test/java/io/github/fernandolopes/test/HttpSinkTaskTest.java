package io.github.fernandolopes.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import static org.mockito.Mockito.*;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkConnectorContext;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.github.fernandolopes.HttpSinkConnect;
import io.github.fernandolopes.HttpSinkTask;

public class HttpSinkTaskTest {
	
	HttpSinkConnect connect;
	Map<String, String> props;
	private SinkTaskContext mockContext;
	
	@BeforeEach
	public void beforeEach() {
	    connect = new HttpSinkConnect();
        
	    props = new HashMap<String, String>();
		
	    
	    
	    props.put("component.https.soTimeout", "30 seconds");
		props.put("sink.url", "https://viacep.com.br");
		props.put("sink.path.httpUri", "/ws/${key}/json/");
		props.put("bootstrap.servers", "localhost:9092");
//		props.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
		props.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
		props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
		props.put("offset.flush.interval.ms", "10000");
		props.put("plugin.path", "/home/connectors");
        props.put("output.data.format", "string");
        props.put("max.retries", "3");
		props.put("tasks.max", "1");
		props.put("topics", "my-topic");
		props.put("group.id", "connect-cluster-sink");
		props.put("sink.endpoint.httpMethod", "GET");
		props.put("internal.value.converter.schemas.enable", "false");
		props.put("sink.endpoint.copyHeaders","true");
		
		var list = new ArrayList<Map<String, String>>();
		list.add(props);
		
	    connect.initialize(new SinkConnectorContext() {
			
			@Override
			public void requestTaskReconfiguration() {
				// TODO Auto-generated method stub

			}
			
			@Override
			public void raiseError(Exception e) {
				// TODO Auto-generated method stub
				
			}
		}, list);
	}
	
	@Test
    public void shouldCreateSinkTask() {
		final var task = new HttpSinkTask();
		String version = task.version();
		assertEquals("8.0.0-ccs", version);
	}
	
	@Test
	public void shouldCreateSinkTaskStart() {

		connect.start(props);
		connect.taskConfigs(1);

		final HttpSinkTask task = new HttpSinkTask();
		
		mockContext = mock(SinkTaskContext.class);
		task.initialize(mockContext);
	    
		task.start(props);
		
		ConnectHeaders headers = new ConnectHeaders();
		headers.addString("traceparent", "00-29643283a6bb6f7d411ed89c950195c2-3dd96f457b15e80a-01");
		
		var content = "{\"userId\": 1, \"name\": \"Fernando\", \"id\": \"60335000\"}";
		
		var record = new SinkRecord(
				"my-topic", 
				1, 
				Schema.STRING_SCHEMA, 
				"\"60864240\"", 
				Schema.BOOLEAN_SCHEMA, 
				content, 
				0L,
                0L, 
                TimestampType.CREATE_TIME, 
                headers, 
                "my-topic", 
                0, 
                0L);
		
		var records = new ArrayList<SinkRecord>();
		records.add(record);
		
		task.put(records);
	}

    @Test
    public void shouldTestNewData() {
        props.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        props.put("key.converter.schema.registry.url","http://james:8082/schema-registry");
        props.put("schemas.enable", "true");
        props.put("internal.value.converter.schemas.enable", "true");
        props.put("sink.path.httpUri", "/ws/${cep}/json/");
        props.put("output.data.format", "json");
        connect.start(props);
        connect.taskConfigs(1);

        final HttpSinkTask task = new HttpSinkTask();

        mockContext = mock(SinkTaskContext.class);
        task.initialize(mockContext);

        task.start(props);

        var content = "{\n" +
                "  \"schema\": {\n" +
                "    \"type\": \"struct\",\n" +
                "    \"fields\": [\n" +
                "      {\"field\": \"cep\", \"type\": \"string\"}\n" +
                "    ]\n" +
                "  },\n" +
                "  \"payload\": {\n" +
                "    \"cep\": \"60864-240\"\n" +
                "  }\n" +
                "}";

        var record = new SinkRecord(
                "my-topic",
                0,
                Schema.STRING_SCHEMA,
                "",
                Schema.BOOLEAN_SCHEMA,
                content,
                0L,
                0L,
                TimestampType.CREATE_TIME,
                null,
                "my-topic",
                0,
                0L);

        var records = new ArrayList<SinkRecord>();
        records.add(record);

        task.put(records);
//	    assertThrows(
//	        RuntimeException.class, // ou a exceção esperada
//	        () -> task.put(records)
//	    );
    }



	@Test
	public void shouldErrorCreateSinkTaskStart() {

		connect.start(props);
		connect.taskConfigs(1);
		
		final HttpSinkTask task = new HttpSinkTask();
		
		mockContext = mock(SinkTaskContext.class);
		task.initialize(mockContext);
		
		task.start(props);
		
		var content = "{\"userId\": 1, \"name\": \"Fernando\", \"id\": \"60335000\"}";
		
		var record = new SinkRecord(
				"my-topic", 
				0, 
				Schema.STRING_SCHEMA, 
				"", 
				Schema.BOOLEAN_SCHEMA, 
				content, 
				0L,
                0L, 
                TimestampType.CREATE_TIME, 
                null, 
                "my-topic", 
                0, 
                0L);
		
		var records = new ArrayList<SinkRecord>();
		records.add(record);

        task.put(records);
//	    assertThrows(
//	        RuntimeException.class, // ou a exceção esperada
//	        () -> task.put(records)
//	    );
	}

}
