package br.com.fernandolopez;

import java.util.Collection;
import java.util.Map;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpConnection;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.impl.Http1StreamListener;
import org.apache.hc.core5.http.impl.bootstrap.HttpRequester;
import org.apache.hc.core5.http.impl.bootstrap.RequesterBootstrap;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.HttpEntities;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.hc.core5.http.message.RequestLine;
import org.apache.hc.core5.http.message.StatusLine;
import org.apache.hc.core5.http.protocol.HttpCoreContext;
import org.apache.hc.core5.util.Timeout;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.fernandolopez.core.Utils;

public class HttpSinkTask extends SinkTask {

	private static final Logger log = LoggerFactory.getLogger(HttpSinkTask.class);
	HttpRequester httpRequester;
	HttpHost target;
	String requestUri;
	
	@Override
	public String version() {
		return new HttpSinkConnect().version();
	}

	@Override
	public void start(Map<String, String> props) {
		log.info("comecou aqui");
		
		HttpSinkConnectConfig config = new HttpSinkConnectConfig(props);
		String data = config.getString(HttpSinkConnectConfig.PMENOS_SINK_HTTPS_COMPONENT_SO_TIMEOUT_CONF);
		
		Timeout time = Utils.getTimeout(data);
		
		log.info("Timeout: : " + data);
		
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
                		.setSoTimeout(time)
                        .build())
                .create();
		
		String urlBase = config.getString(HttpSinkConnectConfig.PMENOS_SINK_URL_CONF);
		target = new HttpHost(urlBase);
		requestUri = config.getString(HttpSinkConnectConfig.PMENOS_SINK_HTTPS_PATH_HTTP_URI_CONF);
		log.info("URL Base: {}", urlBase);
		log.info("rest: {}", requestUri);
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		final HttpCoreContext coreContext = HttpCoreContext.create();
        
		for(SinkRecord record : records) {
			log.info("Escrevendo {}", record.value());
			final HttpEntity body = HttpEntities.create(
	                record.value().toString(),
	                ContentType.TEXT_PLAIN.withCharset(StandardCharsets.UTF_8));
			
			final ClassicHttpRequest request = ClassicRequestBuilder.post()
                    .setHttpHost(target)
                    .setPath(requestUri)
                    .build();
			
            request.setEntity(body);
            try (ClassicHttpResponse response = httpRequester.execute(target, request, Timeout.ofSeconds(5), coreContext)) {
                log.info(requestUri + "->" + response.getCode());
                log.info(EntityUtils.toString(response.getEntity()));
                log.info("==============");
            } catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (HttpException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	@Override
	public void stop() {
		log.info("parou aqui");
	}

}
