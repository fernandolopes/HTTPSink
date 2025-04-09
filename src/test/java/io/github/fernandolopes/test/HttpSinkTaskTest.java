package io.github.fernandolopes.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkConnectorContext;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.github.fernandolopes.HttpSinkConnect;
import io.github.fernandolopes.HttpSinkTask;

public class HttpSinkTaskTest {
	
	HttpSinkConnect connect;
	Map<String, String> props;
	
	@BeforeEach
	public void beforeEach() {
	    connect = new HttpSinkConnect();
	    props = new HashMap<String, String>();

		System.setProperty("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318/v1/traces");
		System.setProperty("OTEL_SERVICE_NAME", "connect-http-sink");
		
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
		assertEquals("3.7.0", version);
	}
	
	@Test
	public void shouldCreateSinkTaskStart() {

		connect.start(props);
		connect.taskConfigs(1);

		final HttpSinkTask task = new HttpSinkTask();
		task.start(props);
		
		ConnectHeaders headers = new ConnectHeaders();
		headers.addString("traceparent", "00-58907994c47bda904a9233c4b8aff021-844f12e2a579d978-01");
		
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
                null, 
                "my-topic", 
                0, 
                0L);
		
		var records = new ArrayList<SinkRecord>();
		records.add(record);
		
		task.put(records);
	}

	@Test
	public void shouldErrorCreateSinkTaskStart() {

		connect.start(props);
		connect.taskConfigs(1);
		
		final HttpSinkTask task = new HttpSinkTask();
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
	}

}
