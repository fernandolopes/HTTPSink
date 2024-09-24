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
import org.apache.kafka.connect.transforms.InsertHeader;
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
		Object obj = new Object();
		
		ConnectHeaders headers = new ConnectHeaders();
//		headers.addString("x-api-version", "v2");
		headers.addString("traceparent", "00-4ec7d23d222cd79740c96bfb01cf22bc-5d171069cca887b9-01");
		
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put("userId", 1);
		map.put("name", "Fernando");
		map.put("age", 45);
		
		var content = "{\"userId\": 1, \"name\": \"Fernando\", \"id\": \"60335000\"}";
		
		var record = new SinkRecord(
				"my-topic", 
				0, 
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
//		records.add(record);
//		records.add(record);
//		records.add(record);
//		records.add(record);
		
		task.put(records);
		//task.stop();
	}

}
