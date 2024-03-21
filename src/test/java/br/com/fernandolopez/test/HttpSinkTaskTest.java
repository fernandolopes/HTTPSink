package br.com.fernandolopez.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkConnectorContext;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import br.com.fernandolopez.HttpSinkConnect;
import br.com.fernandolopez.HttpSinkTask;

public class HttpSinkTaskTest {
	
	HttpSinkConnect connect;
	Map<String, String> props;
	
	@BeforeEach
	public void beforeEach() {
	    connect = new HttpSinkConnect();
	    props = new HashMap<String, String>();
		
	    props.put("pmenos.component.https.soTimeout", "30 seconds");
		props.put("pmenos.sink.url", "api-container.apps.ocp-stg.pmenos.com.br");
		props.put("pmenos.sink.path.httpUri", "/recorder");
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
		props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
		props.put("offset.flush.interval.ms", "10000");
		props.put("plugin.path", "/home/connectors");
		props.put("output.data.format", "json");
		props.put("tasks.max", "1");
		props.put("topics", "my-topic");
		props.put("group.id", "connect-cluster-sink");
		props.put("pmenos.sink.endpoint.httpMethod", "POST");
		props.put("internal.value.converter.schemas.enable", "false");
		
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
		
		var record = new SinkRecord("my-topic", 0, Schema.STRING_SCHEMA, "key", Schema.BOOLEAN_SCHEMA, "{\"id\": 1, \"name\": \"Fernando\"}", 0L,
                0L, TimestampType.CREATE_TIME, null, "my-topic", 0, 0L);
		
		var records = new ArrayList<SinkRecord>();
		records.add(record);
		records.add(record);
		records.add(record);
		records.add(record);
		
		task.put(records);
	}

}
