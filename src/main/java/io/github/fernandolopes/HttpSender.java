package io.github.fernandolopes;

import java.nio.charset.Charset;
import java.util.HashMap;

import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.impl.bootstrap.HttpRequester;
import org.apache.hc.core5.http.io.entity.BasicHttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.hc.core5.http.protocol.HttpCoreContext;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.util.Timeout;
import org.apache.kafka.connect.sink.SinkRecord;

import io.github.fernandolopes.core.Utils;
import io.opentelemetry.api.trace.Span;

public class HttpSender {

	private final HttpRequester httpRequester;
	private final HttpHost target;
	private final Timeout timeout;
    private final String output;
    private final String topic;
    private final String requestUri;
    private final String method;
    private final boolean copyHeaders;

	public HttpSender(HttpSinkConfigManager configManager) throws Exception {
		this.httpRequester = configManager.createHttpRequester();
		this.target = configManager.getHttpHost();
		this.timeout = configManager.getTimeout();
        this.output = configManager.getOutput();
        this.topic = configManager.getTopic();
        this.requestUri = configManager.getRequestUri();
        this.method = configManager.getMethod();
        this.copyHeaders = configManager.getCopyHeaders();
	}

	public void send(SinkRecord record, Span parentSpan) throws Exception {
		ClassicHttpRequest request = getRequested(record);
		try (ClassicHttpResponse response = httpRequester.execute(target, request, timeout, HttpCoreContext.create())) {
			int statusCode = response.getCode();
			if (statusCode != 204) {
				var body = EntityUtils.toString(response.getEntity());
				System.out.println(method + " " + target + request.getPath() + " - " + statusCode + " - " + body);
			}
		}
	}

    private ClassicHttpRequest getRequested(final SinkRecord record) throws Exception {
	    String key = record.key() != null ? record.key().toString() : null;
	    var content = record.value();
	    
	    // Substituir placeholders na URI
	    String requestUri = Utils.replaceRequestUri(this.requestUri, key, topic, output, content);

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

	public void close() {
		httpRequester.close(CloseMode.IMMEDIATE);
	}
}
