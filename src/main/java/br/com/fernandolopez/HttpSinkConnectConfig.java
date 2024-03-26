package br.com.fernandolopez;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class HttpSinkConnectConfig extends AbstractConfig {
	

    public static final String SINK_URL_DEFAULT = null;
    public static final String SINK_URL_CONF = "sink.url";
    public static final String SINK_URL_DOC = "The camel url to configure the destination. If this is set ";

    public static final String SINK_HTTPS_PATH_HTTP_URI_CONF = "sink.path.httpUri";
    public static final String SINK_HTTPS_PATH_HTTP_URI_DOC = "The url of the HTTP endpoint to call.";
    public static final String SINK_HTTPS_PATH_HTTP_URI_DEFAULT = null;
    
    
    public static final String SINK_HTTPS_ENDPOINT_DISABLE_STREAM_CACHE_CONF = "sink.endpoint.disableStreamCache";
    public static final String SINK_HTTPS_ENDPOINT_DISABLE_STREAM_CACHE_DOC = "Determines whether or not the raw input stream from Servlet is cached or not (Camel will read the stream into a in memory/overflow to file, Stream caching) cache. By default Camel will cache the Servlet input stream to support reading it multiple times to ensure it Camel can retrieve all data from the stream. However you can set this option to true when you for example need to access the raw stream, such as streaming it directly to a file or other persistent store. DefaultHttpBinding will copy the request input stream into a stream cache and put it into message body if this option is false to support reading the stream multiple times. If you use Servlet to bridge/proxy an endpoint then consider enabling this option to improve performance, in case you do not need to read the message payload multiple times. The http producer will by default cache the response body stream. If setting this option to true, then the producers will not cache the response body stream but use the response stream as-is as the message body.";
    public static final Boolean SINK_HTTPS_ENDPOINT_DISABLE_STREAM_CACHE_DEFAULT = false;
    
    
    public static final String SINK_HTTPS_ENDPOINT_HEADER_FILTER_STRATEGY_CONF = "sink.endpoint.headerFilterStrategy";
    public static final String SINK_HTTPS_ENDPOINT_HEADER_FILTER_STRATEGY_DOC = "To use a custom HeaderFilterStrategy to filter header to and from Camel message.";
    public static final String SINK_HTTPS_ENDPOINT_HEADER_FILTER_STRATEGY_DEFAULT = null;
    
    
    public static final String SINK_HTTPS_ENDPOINT_BRIDGE_ENDPOINT_CONF = "sink.endpoint.bridgeEndpoint";
    public static final String SINK_HTTPS_ENDPOINT_BRIDGE_ENDPOINT_DOC = "If the option is true, HttpProducer will ignore the Exchange.HTTP_URI header, and use the endpoint's URI for request. You may also set the option throwExceptionOnFailure to be false to let the HttpProducer send all the fault response back.";
    public static final Boolean SINK_HTTPS_ENDPOINT_BRIDGE_ENDPOINT_DEFAULT = false;
    
    public static final String SINK_HTTPS_ENDPOINT_HTTP_METHOD_CONF = "sink.endpoint.httpMethod";
    public static final String SINK_HTTPS_ENDPOINT_HTTP_METHOD_DOC = "Configure the HTTP method to use. The HttpMethod header cannot override this option if set. One of: [GET] [POST] [PUT] [DELETE] [HEAD] [OPTIONS] [TRACE] [PATCH]";
    public static final String SINK_HTTPS_ENDPOINT_HTTP_METHOD_DEFAULT = null;
    
    public static final String SINK_HTTPS_ENDPOINT_COPY_HEADERS_CONF = "sink.endpoint.copyHeaders";
    public static final String SINK_HTTPS_ENDPOINT_COPY_HEADERS_DOC = "If this option is true then IN exchange headers will be copied to OUT exchange headers according to copy strategy. Setting this to false, allows to only include the headers from the HTTP response (not propagating IN headers).";
    public static final Boolean SINK_HTTPS_ENDPOINT_COPY_HEADERS_DEFAULT = true;
    
    public static final Boolean CONNECTOR_MAP_HEADERS_DEFAULT = true;
    public static final String CONNECTOR_MAP_HEADERS_CONF = "map.headers";
    public static final String CONNECTOR_MAP_HEADERS_DOC = "If set to true, the connector will transform the camel exchange headers into kafka headers.";
    
    
    public static final String SINK_HTTPS_ENDPOINT_CUSTOM_HOST_HEADER_CONF = "sink.endpoint.customHostHeader";
    public static final String SINK_HTTPS_ENDPOINT_CUSTOM_HOST_HEADER_DOC = "To use custom host header for producer. When not set in query will be ignored. When set will override host header derived from url.";
    public static final String SINK_HTTPS_ENDPOINT_CUSTOM_HOST_HEADER_DEFAULT = null;

    
    public static final String SINK_HTTPS_COMPONENT_SO_TIMEOUT_CONF = "component.https.soTimeout";
    public static final String SINK_HTTPS_COMPONENT_SO_TIMEOUT_DOC = "Determines the default socket timeout value for blocking I/O operations.";
    public static final String SINK_HTTPS_COMPONENT_SO_TIMEOUT_DEFAULT = "3 minutes";
    
    public static final String SINK_HTTPS_COMPONENT_OUTPUT_DATA_FORMAT_CONF = "output.data.format";
    public static final String SINK_HTTPS_COMPONENT_OUTPUT_DATA_FORMAT_DOC = "output value for the request can be: string, json. default is string";
    public static final String SINK_HTTPS_COMPONENT_OUTPUT_DATA_FORMAT_DEFAULT = "string";
    

    protected HttpSinkConnectConfig(ConfigDef definition, Map<?, ?> originals, Map<String, ?> configProviderProps, boolean doLog) {
        super(definition, originals, configProviderProps, doLog);
        System.out.println("passou no const com tres");
    }
    
	public HttpSinkConnectConfig(ConfigDef definition, Map<?, ?> originals) {
		super(definition, originals);
		System.out.println("passou no const com dois");
	}
	
	public HttpSinkConnectConfig(Map<?, ?> originals) {
		super(HttpSinkConnectConfig.conf(), originals);
		System.out.println("passou no const unico");
	}
	
	public static ConfigDef conf() {
        ConfigDef conf = new ConfigDef();
        
        conf.define(SINK_URL_CONF,                                        Type.STRING,            SINK_URL_DEFAULT,                                        Importance.HIGH,             SINK_URL_DOC);
        conf.define(CONNECTOR_MAP_HEADERS_CONF,                           Type.BOOLEAN,           CONNECTOR_MAP_HEADERS_DEFAULT,                           Importance.MEDIUM,           CONNECTOR_MAP_HEADERS_DOC);
        conf.define(SINK_HTTPS_PATH_HTTP_URI_CONF,                        ConfigDef.Type.STRING,  SINK_HTTPS_PATH_HTTP_URI_DEFAULT,                        ConfigDef.Importance.HIGH,   SINK_HTTPS_PATH_HTTP_URI_DOC);
        conf.define(SINK_HTTPS_ENDPOINT_DISABLE_STREAM_CACHE_CONF,        ConfigDef.Type.BOOLEAN, SINK_HTTPS_ENDPOINT_DISABLE_STREAM_CACHE_DEFAULT,        ConfigDef.Importance.MEDIUM, SINK_HTTPS_ENDPOINT_DISABLE_STREAM_CACHE_DOC);
        conf.define(SINK_HTTPS_ENDPOINT_HEADER_FILTER_STRATEGY_CONF,      ConfigDef.Type.STRING,  SINK_HTTPS_ENDPOINT_HEADER_FILTER_STRATEGY_DEFAULT,      ConfigDef.Importance.MEDIUM, SINK_HTTPS_ENDPOINT_HEADER_FILTER_STRATEGY_DOC);
        conf.define(SINK_HTTPS_ENDPOINT_BRIDGE_ENDPOINT_CONF,             ConfigDef.Type.BOOLEAN, SINK_HTTPS_ENDPOINT_BRIDGE_ENDPOINT_DEFAULT,             ConfigDef.Importance.MEDIUM, SINK_HTTPS_ENDPOINT_BRIDGE_ENDPOINT_DOC);
        conf.define(SINK_HTTPS_ENDPOINT_HTTP_METHOD_CONF,                 ConfigDef.Type.STRING,  SINK_HTTPS_ENDPOINT_HTTP_METHOD_DEFAULT,                 ConfigDef.Importance.MEDIUM, SINK_HTTPS_ENDPOINT_HTTP_METHOD_DOC);
        conf.define(SINK_HTTPS_ENDPOINT_COPY_HEADERS_CONF,                ConfigDef.Type.BOOLEAN, SINK_HTTPS_ENDPOINT_COPY_HEADERS_DEFAULT,                ConfigDef.Importance.MEDIUM, SINK_HTTPS_ENDPOINT_COPY_HEADERS_DOC);
        conf.define(SINK_HTTPS_ENDPOINT_CUSTOM_HOST_HEADER_CONF,          ConfigDef.Type.STRING,  SINK_HTTPS_ENDPOINT_CUSTOM_HOST_HEADER_DEFAULT,          ConfigDef.Importance.MEDIUM, SINK_HTTPS_ENDPOINT_CUSTOM_HOST_HEADER_DOC);
        conf.define(SINK_HTTPS_COMPONENT_SO_TIMEOUT_CONF,                 ConfigDef.Type.STRING,  SINK_HTTPS_COMPONENT_SO_TIMEOUT_DEFAULT,                 ConfigDef.Importance.MEDIUM, SINK_HTTPS_COMPONENT_SO_TIMEOUT_DOC);
        conf.define(SINK_HTTPS_COMPONENT_OUTPUT_DATA_FORMAT_CONF,         ConfigDef.Type.STRING,  SINK_HTTPS_COMPONENT_OUTPUT_DATA_FORMAT_DEFAULT,         ConfigDef.Importance.LOW, SINK_HTTPS_COMPONENT_OUTPUT_DATA_FORMAT_DOC);
        conf.define("topics", 											  ConfigDef.Type.STRING,  null, 												   ConfigDef.Importance.HIGH, "");
        return conf;
	}

}
