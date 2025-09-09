package io.github.fernandolopes;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.fernandolopes.core.TelemetryConfig;
import io.github.fernandolopes.core.Utils;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
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
	private int maxRetries;
	private int retryBackoffMs;
	private ErrantRecordReporter reporter;
	private Tracer tracer = null;
	
	@Override
	public String version() {
		return new HttpSinkConnect().version();
	}

	@Override
	public void start(Map<String, String> props) {
		log.info("Iniciando HttpSinkTask");

		openTelemetry = TelemetryConfig.initOpenTelemetry();
		tracer = openTelemetry.getTracer(HttpSinkTask.class.getName(), "1.0.0");
		
		AbstractConfig config = new AbstractConfig(HttpSinkConnectConfig.conf(), props);
		
		String data = config.getString(HttpSinkConnectConfig.SINK_HTTPS_COMPONENT_SO_TIMEOUT_CONF);
		topics = config.getString("topics");
		output = config.getString(HttpSinkConnectConfig.SINK_HTTPS_COMPONENT_OUTPUT_DATA_FORMAT_CONF);
		
		timeout = Timeout.ofSeconds(30);
		maxRetries = config.getInt(HttpSinkConnectConfig.MAX_RETRIES);
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
		
		URI url = URI.create(urlBase);
		String schema = url.getScheme();
		String host = url.getHost();
		int port = url.getPort();
		
		target = new HttpHost(schema, host, port);
		
		log.info("Schema: " + schema);
		log.info("Host: " + host);
		log.info("Porta: " + port);
		log.info("rest: {}", requestUri);
		log.info("Method: {}", method);
	}

	@Override
	public void put(Collection<SinkRecord> records) {
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

            for (final SinkRecord record : records) {
                processRecord(record);
            }
			
			httpRequester.close();
		
		}
		catch (Exception e)
		{
            log.error("Erro geral no processamento do batch: {}", e.getMessage());
            throw new RetriableException("Falha geral no processamento", e);
		}
	}

    private void processRecord(SinkRecord record) {
        Span consumerSpan = null;
        Span processSpan = null;
        Context extractedContext;
        int recordRetries = maxRetries;

        while (recordRetries > 0) {
            try {
                // Extrair contexto dos headers Kafka usando a implementação correta
                extractedContext = TelemetryConfig.extractContextFromKafkaHeaders(record.headers(), openTelemetry);

                // Criar span consumidor com o contexto extraído
                consumerSpan = tracer.spanBuilder("kafka.consume")
                        .setParent(extractedContext)
                        .setSpanKind(SpanKind.CONSUMER)
                        .setAttribute("messaging.system", "kafka")
                        .setAttribute("messaging.destination", record.topic())
                        .setAttribute("messaging.operation", "receive")
                        .setAttribute("kafka.topic", record.topic())
                        .setAttribute("kafka.partition", record.kafkaPartition())
                        .setAttribute("kafka.offset", record.kafkaOffset())
                        .startSpan();

                // Tornar o span consumidor ativo
                try (Scope consumerScope = consumerSpan.makeCurrent()) {
                    // Criar span de processamento como filho do span consumidor
                    processSpan = tracer.spanBuilder("record.process")
                            .setSpanKind(SpanKind.INTERNAL)
                            .setAttribute("kafka.topic", record.topic())
                            .setAttribute("kafka.partition", record.kafkaPartition())
                            .setAttribute("kafka.offset", record.kafkaOffset())
                            .setAttribute("retry.attempt", maxRetries - recordRetries + 1)
                            .startSpan();

                    try (Scope processScope = processSpan.makeCurrent()) {
                        sendToHttp(record, processSpan);

                        // Sucesso - sair do loop de retry
                        processSpan.setStatus(StatusCode.OK, "Mensagem enviada com sucesso");
                        consumerSpan.setStatus(StatusCode.OK, "Mensagem processada com sucesso");
                        return; // Registro processado com sucesso
                    } finally {
                        processSpan.end();
                    }
                } finally {
                    consumerSpan.end();
                }

            } catch (ConnectException e) {
                recordRetries--;
                log.warn("Falha no envio do registro (tentativa {}/{}): {}", maxRetries - recordRetries, maxRetries, e.getMessage());

                if (processSpan != null) {
                    processSpan.addEvent("Falha na tentativa: " + (maxRetries - recordRetries));
                    processSpan.setStatus(StatusCode.ERROR, "Falha ao enviar mensagem: " + e.getMessage());
                    processSpan.end();
                }

                if (consumerSpan != null) {
                    consumerSpan.addEvent("Retry necessário: " + (maxRetries - recordRetries));
                }

                if (recordRetries > 0) {
                    // Ainda há tentativas restantes - aguardar antes da próxima tentativa
                    try {
                        Thread.sleep(retryBackoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.error("Thread interrompida durante o backoff", ie);
                        break;
                    }
                } else {
                    // Esgotar todas as tentativas - enviar para DLQ
                    log.error("Todas as tentativas esgotadas para o registro. Enviando para DLQ: {}", e.getMessage());

                    if (consumerSpan != null) {
                        consumerSpan.setStatus(StatusCode.ERROR, "Falha após " + maxRetries + " tentativas");
                        consumerSpan.end();
                    }

                    if (reporter != null) {
                        try {
                            reporter.report(record, e);
                            log.info("Registro enviado para DLQ com sucesso");
                        } catch (Exception dlqError) {
                            log.error("Falha ao enviar registro para DLQ: {}", dlqError.getMessage());
                        }
                    } else {
                        log.warn("ErrantRecordReporter não disponível. Registro será perdido.");
                    }
                    return;
                }

            } catch (Exception e) {
                // Erro não recuperável
                log.error("Erro não recuperável no processamento do registro: {}", e.getMessage());

                if (processSpan != null) {
                    processSpan.setStatus(StatusCode.ERROR, "Erro não recuperável: " + e.getMessage());
                    processSpan.end();
                }

                if (consumerSpan != null) {
                    consumerSpan.setStatus(StatusCode.ERROR, "Erro não recuperável");
                    consumerSpan.end();
                }

                if (reporter != null) {
                    try {
                        reporter.report(record, e);
                        log.info("Registro com erro não recuperável enviado para DLQ");
                    } catch (Exception dlqError) {
                        log.error("Falha ao enviar registro para DLQ: {}", dlqError.getMessage());
                    }
                }
                return;
            }
        }
	}

	private void sendToHttp(SinkRecord record, Span parentSpan) throws Exception {

		String data = record.value().toString();
		log.info(data);
		
		ClassicHttpRequest request = getRequested(record);

		HttpCoreContext coreContext = HttpCoreContext.create();
		
		// Criar span HTTP cliente como filho do span de processamento
		Span httpSpan = tracer.spanBuilder("http.client.request")
				.setSpanKind(SpanKind.CLIENT)
				.setAttribute("http.method", request.getMethod())
				.setAttribute("http.url", request.getUri().toString())
				.setAttribute("http.scheme", request.getScheme())
				.setAttribute("http.target", request.getPath())
				.startSpan();

		try (Scope httpScope = httpSpan.makeCurrent()) {
			// Injetar headers de trace na requisição HTTP
			injectTraceHeadersIntoHttpRequest(request, httpSpan);

			try (ClassicHttpResponse response = httpRequester.execute(target, request, timeout, coreContext)) {
				int statusCode = response.getCode();
				log.info(requestUri + " --> " + statusCode);

				// Adicionar atributos de resposta ao span
				httpSpan.setAttribute("http.status_code", statusCode);
				httpSpan.setAttribute("http.response.status_code", statusCode);

				if (statusCode != 204) {
					String payload = EntityUtils.toString(response.getEntity());
					log.info(payload);
					httpSpan.addEvent("Response received");
				}
				log.info("==============");

				Properties prop = new Properties();
				prop.load(HttpSinkTask.class.getClassLoader().getResourceAsStream("config.properties"));
				log.info(prop.getProperty("service.framework.name"));

				// Definir status do span baseado no código de resposta
				if (statusCode >= 200 && statusCode < 300) {
					httpSpan.setStatus(StatusCode.OK, "Request successful");
				} else if (statusCode >= 400) {
					httpSpan.setStatus(StatusCode.ERROR, "HTTP error: " + statusCode);
					throw new ConnectException("Falha na requisição HTTP: " + statusCode);
				}

			} catch (IOException | HttpException e) {
				log.error("Erro na requisição HTTP: {}", e.getMessage());
				httpSpan.setStatus(StatusCode.ERROR, "HTTP request failed: " + e.getMessage());
				httpSpan.recordException(e);
				throw e;
			}
		} finally {
			httpSpan.end();
		}
	}

	/**
	 * Injeta os headers de trace na requisição HTTP para propagação
	 */
	private void injectTraceHeadersIntoHttpRequest(ClassicHttpRequest request, Span span) {
		try {
			// Usar o propagador para injetar headers de trace
			Context currentContext = Context.current().with(span);

			// Criar um mapa para os headers
			Map<String, String> headers = new HashMap<>();

			// Usar o propagador para injetar headers no mapa
			openTelemetry.getPropagators().getTextMapPropagator().inject(
				currentContext,
				headers,
				(carrier, key, value) -> carrier.put(key, value)
			);

			// Adicionar os headers à requisição
			for (Map.Entry<String, String> entry : headers.entrySet()) {
				request.addHeader(entry.getKey(), entry.getValue());
				log.debug("Header de trace injetado: {} = {}", entry.getKey(), entry.getValue());
			}

		} catch (Exception e) {
			log.warn("Erro ao injetar headers de trace: {}", e.getMessage());
		}
	}

	private ClassicHttpRequest getRequested(final SinkRecord record) throws Exception {
	    String key = record.key() != null ? record.key().toString() : null;
	    Object content = record.value();

	    // Substituir placeholders na URI
	    String finalRequestUri = Utils.replaceRequestUri(this.requestUri, key, topics, output, content);

	    ClassicRequestBuilder crb = ClassicRequestBuilder.create(method)
	            .setHttpHost(target)
	            .setPath(finalRequestUri);

	    if (copyHeaders && record.headers() != null) {
		    for (var header : record.headers()) {
		    	if (header.value() != null) {
		    		crb.addHeader(header.key(), header.value().toString());
		    	}
		    }
	    }

	    // Definir o tipo de conteúdo
	    ContentType contentType = output.equals("string") ?
	    		ContentType.TEXT_PLAIN.withCharset(StandardCharsets.UTF_8) :
	    		ContentType.APPLICATION_JSON;

	    if (!method.equals("GET") && content != null) {
	    	if (output.equals("string")) {
	    		crb.setEntity(new StringEntity(content.toString(), contentType));
	    	} else {
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
		log.info("Parando HttpSinkTask");
		if (httpRequester != null) {
			httpRequester.close(CloseMode.IMMEDIATE);
		}
	}

}
