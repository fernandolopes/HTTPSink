package io.github.fernandolopes;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
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
import org.apache.hc.core5.util.Timeout;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.fernandolopes.core.TelemetryConfig;
import io.github.fernandolopes.core.Utils;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;


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
    private Tracer tracer = null;
	
	@Override
	public String version() {
		return new HttpSinkConnect().version();
	}

	@Override
	public void start(Map<String, String> props) {
		log.info("comecou aqui");
		
		
		
		AbstractConfig config = new AbstractConfig(HttpSinkConnectConfig.conf(), props);
		
		String data = config.getString(HttpSinkConnectConfig.SINK_HTTPS_COMPONENT_SO_TIMEOUT_CONF);
		topics = config.getString("topics");
		output = config.getString(HttpSinkConnectConfig.SINK_HTTPS_COMPONENT_OUTPUT_DATA_FORMAT_CONF);
		
		timeout = Timeout.ofSeconds(30);
		
		log.info("Timeout: {}", data);
		
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
		
		target = new HttpHost(urlBase);
		
		log.info("Method: {}", method);
		log.info("URL Base: {}", urlBase);
		log.info("rest: {}", requestUri);
		
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		
		Span span = null;
		openTelemetry = TelemetryConfig.initOpenTelemetry();
		tracer = openTelemetry.getTracer(HttpSinkTask.class.getName(), "1.0.0");
		
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
//				span = TelemetryConfig.getContext(record.headers());
				//Context parentContext = Context.current().with(mainSpan);
				
				//try(Scope scope = span.makeCurrent()) {
					
					span = tracer.spanBuilder("processRecord")
//							.setParent(parentContext)
							.startSpan();
					
	                span.setAttribute("kafka.topic", record.topic());
	                span.setAttribute("kafka.partition", record.kafkaPartition());
	                span.setAttribute("kafka.offset", record.kafkaOffset());
					
					sendToHttp(record, span);
					
//				}
//				finally {
//					if (span != null)
//						span.end();
//		        }
				
			}
			
			httpRequester.close();
		
		} catch (Exception e) {
			log.error(e.getMessage());
			if (span != null) {
				span.setStatus(StatusCode.ERROR, "Falha ao enviar mensagem "+ e);
				span.end();
			}
			throw new RetriableException("Falha ao enviar mensagem", e);
		}

		
	}
	
	
	
	
	private void sendToHttp(SinkRecord record, Span parentSpan) throws Exception {
		
		String data = record.value().toString();
		log.info(data);
		
		var request = getRequested(record);
		
		HttpCoreContext coreContext = HttpCoreContext.create();
		
		try (ClassicHttpResponse response = httpRequester.execute(target, request, timeout, coreContext)) {
            log.info(requestUri + "->" + response.getCode());
            log.info(EntityUtils.toString(response.getEntity()));
            log.info("==============");
            Context parentContext = Context.current().with(parentSpan);
            
            Span reqSpan = tracer.spanBuilder(request.getScheme().toUpperCase() + " "+ request.getMethod() )
    			.setParent(parentContext).startSpan();
            
            reqSpan.setAttribute("http.method", request.getMethod());
            reqSpan.setAttribute("http.scheme", request.getScheme());
            reqSpan.setAttribute("net.peer.name", request.getRequestUri());
            reqSpan.setAttribute("http.url", request.getUri().toString());
            reqSpan.setAttribute("http.status_code", response.getCode());
            reqSpan.end();
            parentSpan.end();
        } catch (IOException e) {
        	log.error(e.getMessage());
			e.printStackTrace();
		} catch (HttpException e) {
			log.error(e.getMessage());
			e.printStackTrace();
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
		
	}

}
