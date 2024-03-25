package br.com.fernandolopez;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpSinkConnect extends SinkConnector {

	private static final Logger log = LoggerFactory.getLogger(HttpSinkConnect.class);
	private Map<String, String> props;
	
	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

	@Override
	public void start(Map<String, String> configProps) {
		
		String resultString ="" ;
		for(Map.Entry<String,String> entry : configProps.entrySet())
		{
			String key = entry.getKey();
			String value = entry.getValue();
			String testString = "        " + key + " = " + value;
			resultString +=  testString + "\n";
		}
		
		
		log.info("My Connector config keys: {}", resultString);
        this.props = configProps;
	}

	@Override
	public Class<? extends Task> taskClass() {
		return HttpSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		log.info("Definir configuracoes de task para {} workers.", maxTasks);
		ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(props);
        }
        return configs;
	}

	@Override
	public void stop() {
		log.info("passa no stop do connect");
	}

	@Override
	public ConfigDef config() {
		return HttpSinkConnectConfig.conf();
	}

}
