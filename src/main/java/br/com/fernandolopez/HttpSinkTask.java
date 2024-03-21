package br.com.fernandolopez;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpConnection;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.Message;
import org.apache.hc.core5.http.impl.Http1StreamListener;
import org.apache.hc.core5.http.impl.bootstrap.AsyncRequesterBootstrap;
import org.apache.hc.core5.http.impl.bootstrap.HttpAsyncRequester;
import org.apache.hc.core5.http.impl.bootstrap.HttpRequester;
import org.apache.hc.core5.http.impl.bootstrap.RequesterBootstrap;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.HttpEntities;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.hc.core5.http.message.RequestLine;
import org.apache.hc.core5.http.message.StatusLine;
import org.apache.hc.core5.http.nio.AsyncClientEndpoint;
import org.apache.hc.core5.http.nio.AsyncRequestProducer;
import org.apache.hc.core5.http.nio.entity.StringAsyncEntityConsumer;
import org.apache.hc.core5.http.nio.support.AsyncRequestBuilder;
import org.apache.hc.core5.http.nio.support.BasicResponseConsumer;
import org.apache.hc.core5.http.protocol.HttpCoreContext;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.Timeout;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import br.com.fernandolopez.core.Utils;


public class HttpSinkTask extends SinkTask {

	private static final Logger log = LoggerFactory.getLogger(HttpSinkTask.class);
	HttpAsyncRequester httpRequester;
	HttpHost target;
	String requestUri = null;
	String method = null;
	//String output;
	
	@Override
	public String version() {
		return new HttpSinkConnect().version();
	}

	@Override
	public void start(Map<String, String> props) {
		log.info("comecou aqui");
		
		HttpSinkConnectConfig config = new HttpSinkConnectConfig(props);
		String data = config.getString(HttpSinkConnectConfig.PMENOS_SINK_HTTPS_COMPONENT_SO_TIMEOUT_CONF);
		
		//String valueConverter = config.getString("value.converter");
		//output = config.getString("output.data.format");
		
		Timeout time = Timeout.ofMilliseconds(30); //Utils.getTimeout(data);
		final IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                .setSoTimeout(time)
                .build();
		
		log.info("Timeout: {}", data);
		//log.info("Value converter: {}", valueConverter);
		httpRequester = AsyncRequesterBootstrap.bootstrap()
				.setIOReactorConfig(ioReactorConfig)
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
//                .setSocketConfig(SocketConfig.custom()
//                		.setSoTimeout(time)
//                        .build())
                .create();
		
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("HTTP requester shutting down");
            httpRequester.close(CloseMode.GRACEFUL);
        }));
		httpRequester.start();
		
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
//		final HttpCoreContext coreContext = HttpCoreContext.create();
		
		final Future<AsyncClientEndpoint> future = httpRequester.connect(target, Timeout.ofSeconds(5));
		try {
			final AsyncClientEndpoint clientEndpoint = future.get();
			final CountDownLatch latch = new CountDownLatch(records.size());
		
        
			for(SinkRecord record : records) {
				log.info("Escrevendo {}", record.value());
				
				AsyncRequestProducer request = getRequested(record);
				
				clientEndpoint.execute(
	                    request,
	                    new BasicResponseConsumer<>(new StringAsyncEntityConsumer()),
	                    new FutureCallback<Message<HttpResponse, String>>() {

	                        @Override
	                        public void completed(final Message<HttpResponse, String> message) {
	                            latch.countDown();
	                            final HttpResponse response = message.getHead();
	                            final String body = message.getBody();
	                            log.info(requestUri + "->" + response.getCode());
	                            log.info(body);
	                            log.info("==============");
	                        }

	                        @Override
	                        public void failed(final Exception ex) {
	                            latch.countDown();
	                            System.out.println(requestUri + "->" + ex);
	                        }

	                        @Override
	                        public void cancelled() {
	                            latch.countDown();
	                            System.out.println(requestUri + " cancelled");
	                        }

	                    });
	        }

	        latch.await();
	        
	        clientEndpoint.releaseAndDiscard();
	        httpRequester.initiateShutdown();
	            
	//            try (ClassicHttpResponse response = httpRequester.execute(target, request, Timeout.ofSeconds(5), coreContext)) {
	//                log.info(requestUri + "->" + response.getCode());
	//                log.info(EntityUtils.toString(response.getEntity()));
	//                log.info("==============");
	//            } catch (IOException e) {
	//				// TODO Auto-generated catch block
	//				e.printStackTrace();
	//			} catch (HttpException e) {
	//				// TODO Auto-generated catch block
	//				e.printStackTrace();
	//			}
	//		}
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private AsyncRequestProducer getRequested(final SinkRecord record) {
//		final HttpEntity body = HttpEntities.create(
//                record.value().toString(),
//                ContentType.TEXT_PLAIN.withCharset(StandardCharsets.UTF_8));
		
		
		ContentType contentType;
		byte[] content = record.value().toString().getBytes(StandardCharsets.UTF_8);
		
//		if (output.equals("string")) {
//			contentType = ContentType.TEXT_PLAIN;
//		} else {
			contentType = ContentType.APPLICATION_JSON;
//		}
		
		AsyncRequestProducer request = null;

		if (method.equals("GET")) {
			request = AsyncRequestBuilder.get()
					.setHttpHost(target)
		            .setPath(requestUri)
		            .build();
		}
		else if (method.equals("POST")) {
			request = AsyncRequestBuilder.post()
					.setHttpHost(target)
					.setEntity(content, contentType)
		            .setPath(requestUri)
		            .build();
		}
		else if (method.equals("PUT")) {
			request = AsyncRequestBuilder.put()
					.setHttpHost(target)
		            .setPath(requestUri)
		            .build();
		}
		else if (method.equals("PATCH")) {
			request = AsyncRequestBuilder.patch()
					.setHttpHost(target)
		            .setPath(requestUri)
		            .build();
		}
		else if (method.equals("DELETE")) {
			request = AsyncRequestBuilder.delete()
					.setHttpHost(target)
		            .setPath(requestUri)
		            .build();
		}
		return request;
	}

	@Override
	public void stop() {
		log.info("parou aqui");
	}

}
