package io.github.fernandolopes;

import java.io.IOException;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpConnection;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.impl.Http1StreamListener;
import org.apache.hc.core5.http.impl.bootstrap.HttpRequester;
import org.apache.hc.core5.http.impl.bootstrap.RequesterBootstrap;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.http.io.entity.BasicHttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.hc.core5.http.message.RequestLine;
import org.apache.hc.core5.http.message.StatusLine;
import org.apache.hc.core5.http.protocol.HttpCoreContext;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.util.Timeout;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.fernandolopes.core.TelemetryConfig;
import io.github.fernandolopes.core.Utils;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;


public class HttpSinkTask extends SinkTask {

	private static final Logger log = LoggerFactory.getLogger(HttpSinkTask.class);
	private HttpRequester httpRequester;
	private HttpHost target;
	private String requestUri = null;
	private String method = null;
	private String output = null;
	private Timeout timeout;
	private String topics;
	private boolean copyHeaders = true;
	private OpenTelemetry openTelemetry = null;
	int remainingRetries;
	int maxRetries;
	int retryBackoffMs;
	private ErrantRecordReporter reporter;
	private Tracer tracer = null;
	
	@Override
	public String version() {
		return new HttpSinkConnect().version();
	}

	@Override
	public void start(Map<String, String> props) {
		log.info("comecou aqui");
		
		openTelemetry = TelemetryConfig.initOpenTelemetry();
		tracer = openTelemetry.getTracer(HttpSinkTask.class.getName(), "1.0.0");
		
		AbstractConfig config = new AbstractConfig(HttpSinkConnectConfig.conf(), props);
		
		String data = config.getString(HttpSinkConnectConfig.SINK_HTTPS_COMPONENT_SO_TIMEOUT_CONF);
		topics = config.getString("topics");
		output = config.getString(HttpSinkConnectConfig.SINK_HTTPS_COMPONENT_OUTPUT_DATA_FORMAT_CONF);
		
		timeout = Timeout.ofSeconds(30);
		remainingRetries = config.getInt(HttpSinkConnectConfig.MAX_RETRIES);
		maxRetries = remainingRetries;
		retryBackoffMs = config.getInt(HttpSinkConnectConfig.RETRY_BACKOFF_MS);
		log.info("Timeout: {}", data);
		
		if (context != null) {
            try {
                reporter = context.errantRecordReporter();
            } catch (NoSuchMethodError | NoClassDefFoundError e) {
                log.warn("Unable to instantiate ErrantRecordReporter.  Method 'SinkTaskContext.errantRecordReporter' does not exist.");
                reporter = null;
            }
        }
		
		String urlBase = config.getString(HttpSinkConnectConfig.SINK_URL_CONF);
		requestUri = config.getString(HttpSinkConnectConfig.SINK_HTTPS_PATH_HTTP_URI_CONF);
		method = config.getString(HttpSinkConnectConfig.SINK_HTTPS_ENDPOINT_HTTP_METHOD_CONF);
		
		try {
			if(!method.equals("GET") && 
			   !method.equals("POST") &&
			   !method.equals("PUT") &&
			   !method.equals("PATCH") &&
			   !method.equals("DELETE") &&
			   !method.equals("TRACE") &&
			   !method.equals("OPTIONS") &&
			   !method.equals("HEAD")) {
				throw new Exception("Error method not suported.: " + method);
			}
		} 
		catch (Exception e) {
			log.error(e.getMessage());
			throw new RetriableException("Falha ao enviar mensagem", e);
		}
		
		copyHeaders = config.getBoolean(HttpSinkConnectConfig.SINK_HTTPS_ENDPOINT_COPY_HEADERS_CONF);
		
		try {
            URL url = new URL(urlBase);
            String schema = url.getProtocol();
            String host = url.getHost();
            int port = url.getPort();
            
            // Se a porta não estiver definida, URL.getPort() retorna -1, então definimos a porta padrão com base no protocolo.
//            if (port == -1) {
//                port = schema.equals("https") ? 443 : 80;
//            }
            target = new HttpHost(schema, host, port);
            
            log.info("Schema: " + schema);
            log.info("Host: " + host);
            log.info("Porta: " + port);
            log.info("rest: {}", requestUri);
            log.info("Method: {}", method);
            
        } catch (MalformedURLException e) {
            // Retorna null ou lança uma exceção se a URL for inválida
            System.err.println("URL inválida: " + e.getMessage());
        }
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		
		Span span = null;
		
		if (records.isEmpty()) {
	      return;
	    }
		
		
		try {
			
			httpRequester = RequesterBootstrap.bootstrap()
	                .setStreamListener(new Http1StreamListener() {

	                    @Override
	                    public void onRequestHead(final HttpConnection connection, final HttpRequest request) {
	                        log.info(connection.getRemoteAddress() + " " + new RequestLine(request));
	                    }

	                    @Override
	                    public void onResponseHead(final HttpConnection connection, final HttpResponse response) {
	                        log.info(connection.getRemoteAddress() + " " + new StatusLine(response));
	                    }

	                    @Override
	                    public void onExchangeComplete(final HttpConnection connection, final boolean keepAlive) {
	                    	
	                        if (keepAlive) {
	                            log.info(connection.getRemoteAddress() + " exchange completed (connection kept alive)");
	                        } else {
	                            log.info(connection.getRemoteAddress() + " exchange completed (connection closed)");
	                        }
	                    }

	                })
	                .setSocketConfig(SocketConfig.custom()
	                        .setSoTimeout(30, TimeUnit.SECONDS)
	                        .build())
	                .create();
			
			for(final SinkRecord record : records) {
				var mainSpan = TelemetryConfig.getContext(record.headers());
				Context parentContext = Context.current().with(mainSpan);
				
				try(Scope scope = mainSpan.makeCurrent()) {
					
					span = tracer.spanBuilder("processRecord")
							.setParent(parentContext)
							.startSpan();
					
	                span.setAttribute("kafka.topic", record.topic());
	                span.setAttribute("kafka.partition", record.kafkaPartition());
	                span.setAttribute("kafka.offset", record.kafkaOffset());
					
					sendToHttp(record, span);
					span.end();
					
					if (mainSpan != null)
						mainSpan.end();
				}
				catch(ConnectException e) {
					log.error("falha controlada de conectividade: {}",e.getMessage());
					if (span != null) {
						span.setStatus(StatusCode.ERROR, "Falha ao enviar mensagem "+ e);
						span.setAttribute("otel.status_code", "ERROR");
						span.setAttribute("otel.status_description", "Falha ao enviar mensagem "+ e);
						span.end();
					}
					if (remainingRetries > 0) {
						remainingRetries--;
				        context.timeout(retryBackoffMs);
						throw new RetriableException("Falha ao enviar mensagem", e);
					}
					remainingRetries = maxRetries;
					log.warn("A delivery has failed and the error reporting is enabled. Sending record to the DLQ");
		            reporter.report(record, e);
				}
				
			}
			
			httpRequester.close();
		
		}
		catch (Exception e)
		{
			log.error("falha controlada: {}", e.getMessage());
			if (span != null) {
				span.setStatus(StatusCode.ERROR, "Falha ao enviar mensagem "+ e);
				span.setAttribute("otel.status_code", "ERROR");
				span.setAttribute("otel.status_description", "Falha ao enviar mensagem "+ e);
				span.end();
			}
			
			throw new RetriableException("Falha ao enviar mensagem", e);
		}
	}
	
	
	
	
	private void sendToHttp(SinkRecord record, Span parentSpan) throws Exception, ConnectException {
		
		String data = record.value().toString();
		log.info(data);
		
		var request = getRequested(record);
		
		HttpCoreContext coreContext = HttpCoreContext.create();
		
		try (ClassicHttpResponse response = httpRequester.execute(target, request, timeout, coreContext)) {
            log.info(requestUri + "->" + response.getCode());
            log.info(EntityUtils.toString(response.getEntity()));
            log.info("==============");
            Context parentContext = Context.current().with(parentSpan);
            String path = request.getScheme().toUpperCase() + " "+ request.getMethod();
            Span reqSpan = tracer.spanBuilder(path)
    			.setParent(parentContext)
    			.startSpan();
            
//            reqSpan.setAttribute("http.method", request.getMethod());
//            reqSpan.setAttribute("http.scheme", request.getScheme());
//            reqSpan.setAttribute("net.peer.name", request.getRequestUri());
//            reqSpan.setAttribute("http.url", request.getUri().toString());
//            reqSpan.setAttribute("http.status_code", response.getCode());

			Properties prop = new Properties();
			prop.load(HttpSinkTask.class.getClassLoader().getResourceAsStream("config.properties"));
			//get the property value and print it out
			System.out.println(prop.getProperty("service.framework.name"));

			reqSpan.setAttribute("http.request.method", request.getMethod());
			reqSpan.setAttribute("http.response.status_code", response.getCode());
			reqSpan.setAttribute("url.full", request.getUri().toString());
			reqSpan.setAttribute("url.original", request.getUri().toString());
			reqSpan.setAttribute("url.path", request.getUri().toString());
			reqSpan.setAttribute("url.schema", request.getScheme());
			reqSpan.setAttribute("service.language", "java");
			reqSpan.setAttribute("service.framework.name", prop.getProperty("service.framework.name"));
			reqSpan.setAttribute("otel.library.version", prop.getProperty("otel.library.version"));
			reqSpan.setAttribute("service.name", "Connect-Sink");
			reqSpan.setAttribute("span.kind", "client");
			reqSpan.setStatus(StatusCode.OK, "Requested successfully");
			reqSpan.setAttribute("otel.status_code", "OK");
            reqSpan.end();
        } catch (IOException e) {
        	log.error(e.getMessage());
			e.printStackTrace();
			throw e;
		} catch (HttpException e) {
			log.error(e.getMessage());
			e.printStackTrace();
			throw e;
		}
		
	}
	
	private ClassicHttpRequest getRequested(final SinkRecord record) throws Exception {
	    String key = record.key() != null ? record.key().toString() : null;
	    var content = record.value();
	    
	    // Substituir placeholders na URI
	    String requestUri = Utils.replaceRequestUri(this.requestUri, key, topics, output, content);

	    ClassicRequestBuilder crb = ClassicRequestBuilder.create(method)
	            .setHttpHost(target)
	            .setPath(requestUri);
	    
	    if (copyHeaders) {
		    for (var header : record.headers()) {
		    	crb.addHeader(header.key(), header.value().toString());
		    }
	    }
	    
	    
	    // Definir o tipo de conteúdo
	    ContentType contentType = output.equals("string") ? ContentType.TEXT_PLAIN.withCharset(Charset.forName("UTF-8")) : ContentType.APPLICATION_JSON;

	    if (!method.equals("GET")) {
	    	if (output.equals("string"))
	    		crb.setEntity(new StringEntity(content.toString(), contentType));
	    	else
	    	{
	    		@SuppressWarnings("unchecked")
				HashMap<String, Object> map = (HashMap<String, Object>) content;
    			var input = Utils.convertToInputStream(map);
	    		
	    		BasicHttpEntity entity = new BasicHttpEntity(input, contentType);
	    		
			    crb.setEntity(entity);		
	    		
	    	}
	    }

	    return crb.build();
	}

	@Override
	public void stop() {
		log.info("parou aqui");
		httpRequester.close(CloseMode.IMMEDIATE);
	}

}
