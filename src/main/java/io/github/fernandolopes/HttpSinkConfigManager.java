package io.github.fernandolopes;

import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.impl.bootstrap.HttpRequester;
import org.apache.hc.core5.http.impl.bootstrap.RequesterBootstrap;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.util.Timeout;
import org.apache.kafka.common.config.AbstractConfig;

import io.github.fernandolopes.core.TelemetryConfig;
import io.opentelemetry.api.OpenTelemetry;

import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HttpSinkConfigManager {

	private final AbstractConfig config;

	public HttpSinkConfigManager(Map<String, String> props) {
		this.config = new AbstractConfig(HttpSinkConnectConfig.conf(), props);
	}

	public HttpRequester createHttpRequester() {
		return RequesterBootstrap.bootstrap()
				.setSocketConfig(SocketConfig.custom().setSoTimeout(30, TimeUnit.SECONDS).build())
				.create();
	}

	public HttpHost getHttpHost() throws Exception {
		@SuppressWarnings("deprecation")
        URL url = new URL(config.getString(HttpSinkConnectConfig.SINK_URL_CONF));
		return new HttpHost(url.getProtocol(), url.getHost(), url.getPort());
	}

	public Timeout getTimeout() {
		return Timeout.ofSeconds(30);
	}

	public int getMaxRetries() {
		return config.getInt(HttpSinkConnectConfig.MAX_RETRIES);
	}

	public int getRetryBackoffMs() {
		return config.getInt(HttpSinkConnectConfig.RETRY_BACKOFF_MS);
	}

	public OpenTelemetry getOpenTelemetry() {
		return TelemetryConfig.initOpenTelemetry();
	}

    public String getOutput() {
        return config.getString(HttpSinkConnectConfig.SINK_HTTPS_COMPONENT_OUTPUT_DATA_FORMAT_CONF);
    }

    public String getTopic() {
        return config.getString("topics");
    }

    public String getRequestUri() {
        return config.getString(HttpSinkConnectConfig.SINK_HTTPS_PATH_HTTP_URI_CONF);
    }

    public String getMethod() {
        return config.getString(HttpSinkConnectConfig.SINK_HTTPS_ENDPOINT_HTTP_METHOD_CONF);
    }

    public boolean getCopyHeaders() {
        return config.getBoolean(HttpSinkConnectConfig.SINK_HTTPS_ENDPOINT_COPY_HEADERS_CONF) | true;
    }
}
