package br.com.fernandolopez;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
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
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.hc.core5.http.message.RequestLine;
import org.apache.hc.core5.http.message.StatusLine;
import org.apache.hc.core5.http.protocol.HttpCoreContext;
import org.apache.hc.core5.util.Timeout;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.fernandolopez.core.Utils;


public class HttpSinkTask extends SinkTask {

	private static final Logger log = LoggerFactory.getLogger(HttpSinkTask.class);
	private HttpRequester httpRequester;
	private HttpHost target;
	private String requestUri = null;
	private String method = null;
	private String output = null;
	private Timeout timeout;
	private String topics;
	
	@Override
	public String version() {
		return new HttpSinkConnect().version();
	}

	@Override
	public void start(Map<String, String> props) {
		log.info("comecou aqui");
		
		AbstractConfig config = new AbstractConfig(HttpSinkConnectConfig.conf(), props);
		
		String data = config.getString(HttpSinkConnectConfig.PMENOS_SINK_HTTPS_COMPONENT_SO_TIMEOUT_CONF);
		topics = config.getString("topics");
		output = config.getString(HttpSinkConnectConfig.PMENOS_SINK_HTTPS_COMPONENT_OUTPUT_DATA_FORMAT_CONF);
		
		timeout = Timeout.ofSeconds(30);
		
		log.info("Timeout: {}", data);
		
		String urlBase = config.getString(HttpSinkConnectConfig.PMENOS_SINK_URL_CONF);
		requestUri = config.getString(HttpSinkConnectConfig.PMENOS_SINK_HTTPS_PATH_HTTP_URI_CONF);
		method = config.getString(HttpSinkConnectConfig.PMENOS_SINK_HTTPS_ENDPOINT_HTTP_METHOD_CONF);
		target = new HttpHost(urlBase);
		
		log.info("Method: {}", method);
		log.info("URL Base: {}", urlBase);
		log.info("rest: {}", requestUri);
		
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		try {
			httpRequester = RequesterBootstrap.bootstrap()
	                .setStreamListener(new Http1StreamListener() {

	                    @Override
	                    public void onRequestHead(final HttpConnection connection, final HttpRequest request) {
	                        System.out.println(connection.getRemoteAddress() + " " + new RequestLine(request));

	                    }

	                    @Override
	                    public void onResponseHead(final HttpConnection connection, final HttpResponse response) {
	                        System.out.println(connection.getRemoteAddress() + " " + new StatusLine(response));
	                    }

	                    @Override
	                    public void onExchangeComplete(final HttpConnection connection, final boolean keepAlive) {
	                        if (keepAlive) {
	                            System.out.println(connection.getRemoteAddress() + " exchange completed (connection kept alive)");
	                        } else {
	                            System.out.println(connection.getRemoteAddress() + " exchange completed (connection closed)");
	                        }
	                    }

	                })
	                .setSocketConfig(SocketConfig.custom()
	                        .setSoTimeout(30, TimeUnit.SECONDS)
	                        .build())
	                .create();
			
			for(final SinkRecord record : records) {
				sendToHttp(record);
			}
			
			httpRequester.close();
		
		} catch (Exception e) {
			log.error(e.getMessage());
			throw new RetriableException("Falha ao enviar mensagem", e);
		}
		
	}
	
	
	private void sendToHttp(SinkRecord record) throws Exception {
		String data = record.value().toString();
		log.info(data);
		
		var request = getRequested(record);
		
		HttpCoreContext coreContext = HttpCoreContext.create();
		
		try (ClassicHttpResponse response = httpRequester.execute(target, request, timeout, coreContext)) {
            log.info(requestUri + "->" + response.getCode());
            log.info(EntityUtils.toString(response.getEntity()));
            log.info("==============");
        } catch (IOException e) {
        	log.error(e.getMessage());
			e.printStackTrace();
		} catch (HttpException e) {
			log.error(e.getMessage());
			e.printStackTrace();
		}
		
	}

//	private ClassicHttpRequest getRequested(final SinkRecord record) throws Exception {
//		//requestUri = replaceRequestUri(requestUri, record);
//		
//		requestUri = Utils.replaceRequestUri(requestUri,
//				(record.key() != null)? record.key().toString() : null,
//				topics,
//				output,
//				record.value());
//		
//		if (method.equals("GET")) {
//			return ClassicRequestBuilder.get()
//					.setHttpHost(target)
//		            .setPath(requestUri)
//		            .build();
//		}
//		
//		ContentType contentType = null;
//		byte[] content = null;
//
//		content = record.value().toString().getBytes(StandardCharsets.UTF_8);
//		
//		if (output.equals("string")) {
//			contentType = ContentType.TEXT_PLAIN;
//		} else {
//			contentType = ContentType.APPLICATION_JSON;
//		}
//
//		
//		ClassicRequestBuilder crb = null;
//		
//		if (method.equals("POST")) {
//			crb = ClassicRequestBuilder.post();
//		}
//		else if (method.equals("PUT")) {
//			crb = ClassicRequestBuilder.put();
//		}
//		else if (method.equals("PATCH")) {
//			crb = ClassicRequestBuilder.patch();
//		}
//		else if (method.equals("DELETE")) {
//			crb = ClassicRequestBuilder.delete();
//		}
//		
//		return crb
//				.setHttpHost(target)
//				.setEntity(content, contentType)
//	            .setPath(requestUri)
//	            .build();
//	}
	
	private ClassicHttpRequest getRequested(final SinkRecord record) throws Exception {
	    String key = record.key() != null ? record.key().toString() : null;
	    String content = record.value().toString();
	    
	    // Substituir placeholders na URI
	    String requestUri = Utils.replaceRequestUri(this.requestUri, key, topics, output, content);

	    ClassicRequestBuilder crb = ClassicRequestBuilder.create(method)
	            .setHttpHost(target)
	            .setPath(requestUri);

	    // Definir o tipo de conteúdo
	    ContentType contentType = output.equals("string") ? ContentType.TEXT_PLAIN : ContentType.APPLICATION_JSON;

	    if (!method.equals("GET")) {
	        crb.setEntity(new StringEntity(content, contentType));
	    }

	    return crb.build();
	}

	@Override
	public void stop() {
		log.info("parou aqui");
		
	}

}
