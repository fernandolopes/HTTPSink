package io.github.fernandolopes;

import io.github.fernandolopes.core.Utils;
import io.github.fernandolopes.telemetry.TelemetryManager;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.apache.avro.generic.GenericRecord;
import org.apache.hc.core5.http.*;
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
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class HttpSinkTask extends SinkTask {

	private static final Logger log = LoggerFactory.getLogger(HttpSinkTask.class);
	private HttpRequester httpRequester;
	private HttpHost target;
	private String requestUri = null;
	private String method = null;
	private String output = null;
    private String converter = null;
	private Timeout timeout;
	private String topics;
	private boolean copyHeaders = true;
	private int maxRetries;
	private int retryBackoffMs;
	private ErrantRecordReporter reporter;
	private TelemetryManager telemetryManager;

	@Override
	public String version() {
		return new HttpSinkConnect().version();
	}

	@Override
	public void start(Map<String, String> props) {
		log.info("Iniciando HttpSinkTask");

		// Inicializar telemetria
		telemetryManager = TelemetryManager.getInstance();
		telemetryManager.initialize(props);

		AbstractConfig config = new AbstractConfig(HttpSinkConnectConfig.conf(), props);

		String data = config.getString(HttpSinkConnectConfig.SINK_HTTPS_COMPONENT_SO_TIMEOUT_CONF);
		topics = config.getString(HttpSinkConnectConfig.SINK_HTTPS_COMPONENT_TOPICS_CONF);
		output = config.getString(HttpSinkConnectConfig.SINK_HTTPS_COMPONENT_OUTPUT_DATA_FORMAT_CONF);
        converter = config.getString(HttpSinkConnectConfig.SINK_HTTPS_COMPONENT_VALUE_CONVERTER_CONF);
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

		// Para o batch, usar o contexto do primeiro record se disponível
		SinkRecord firstRecord = records.iterator().next();
		Context batchContext = telemetryManager.extractContextFromHeaders(firstRecord.headers());

		// Criar span para o batch de registros usando o contexto extraído
		Span batchSpan = telemetryManager.createSpanWithContext("kafka-connect-batch-process", batchContext);
		telemetryManager.addSpanAttribute(batchSpan, "batch.size", records.size());
		telemetryManager.addSpanAttribute(batchSpan, "connector.name", "http-sink");

		try (Scope batchScope = telemetryManager.activateSpan(batchSpan)) {
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

			telemetryManager.addSpanAttribute(batchSpan, "batch.status", "success");
			batchSpan.setStatus(StatusCode.OK);

		}
		catch (Exception e)
		{
			telemetryManager.recordException(batchSpan, e);
			telemetryManager.addSpanAttribute(batchSpan, "batch.status", "error");
			batchSpan.setStatus(StatusCode.ERROR, e.getMessage());

            log.error("Erro geral no processamento do batch: {}", e.getMessage());
            throw new RetriableException("Falha geral no processamento", e);
		} finally {
			telemetryManager.finishSpan(batchSpan);
		}
	}

    private void processRecord(SinkRecord record) {
        // Extrair contexto dos headers do record atual
        Context recordContext = telemetryManager.extractContextFromHeaders(record.headers());

        // Criar span para cada registro usando o contexto extraído
        Span recordSpan = telemetryManager.createSpanWithContext("kafka-connect-record-process", recordContext);
        telemetryManager.addSpanAttribute(recordSpan, "kafka.topic", record.topic());
        telemetryManager.addSpanAttribute(recordSpan, "kafka.partition", record.kafkaPartition());
        telemetryManager.addSpanAttribute(recordSpan, "kafka.offset", record.kafkaOffset());

        try (Scope recordScope = telemetryManager.activateSpan(recordSpan)) {
            int recordRetries = maxRetries;

            while (recordRetries > 0) {
                try {
                    sendToHttp(record);

                    // Sucesso - sair do loop de retry
                    telemetryManager.addSpanAttribute(recordSpan, "record.status", "success");
                    telemetryManager.addSpanAttribute(recordSpan, "retry.attempts", maxRetries - recordRetries);
                    recordSpan.setStatus(StatusCode.OK);
                    return; // Registro processado com sucesso
                } catch (ConnectException e) {
                    recordRetries--;
                    telemetryManager.addSpanAttribute(recordSpan, "retry.current", maxRetries - recordRetries + 1);
                    log.warn("Falha no envio do registro (tentativa {}/{}): {}", maxRetries - recordRetries, maxRetries, e.getMessage());

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

                        telemetryManager.recordException(recordSpan, e);
                        telemetryManager.addSpanAttribute(recordSpan, "record.status", "failed_to_dlq");
                        telemetryManager.addSpanAttribute(recordSpan, "retry.attempts", maxRetries);
                        recordSpan.setStatus(StatusCode.ERROR, "Max retries exceeded");

                        if (reporter != null) {
                            try {
                                reporter.report(record, e);
                                log.info("Registro enviado para DLQ com sucesso");
                                telemetryManager.addSpanAttribute(recordSpan, "dlq.sent", true);
                            } catch (Exception dlqError) {
                                log.error("Falha ao enviar registro para DLQ: {}", dlqError.getMessage());
                                telemetryManager.addSpanAttribute(recordSpan, "dlq.sent", false);
                                telemetryManager.recordException(recordSpan, dlqError);
                            }
                        } else {
                            log.warn("ErrantRecordReporter não disponível. Registro será perdido.");
                            telemetryManager.addSpanAttribute(recordSpan, "dlq.available", false);
                        }
                        return;
                    }

                } catch (Exception e) {
                    // Erro não recuperável
                    log.error("Erro não recuperável no processamento do registro: {}", e.getMessage());

                    telemetryManager.recordException(recordSpan, e);
                    telemetryManager.addSpanAttribute(recordSpan, "record.status", "error_non_recoverable");
                    recordSpan.setStatus(StatusCode.ERROR, "Non-recoverable error");

                    if (reporter != null) {
                        try {
                            reporter.report(record, e);
                            log.info("Registro com erro não recuperável enviado para DLQ");
                            telemetryManager.addSpanAttribute(recordSpan, "dlq.sent", true);
                        } catch (Exception dlqError) {
                            log.error("Falha ao enviar registro para DLQ: {}", dlqError.getMessage());
                            telemetryManager.addSpanAttribute(recordSpan, "dlq.sent", false);
                        }
                    }
                    return;
                }
            }
        } finally {
            telemetryManager.finishSpan(recordSpan);
        }
	}

	private void sendToHttp(SinkRecord record) throws Exception {
		// O span HTTP deve ser filho do span do record (que já está ativo)
		// Usar o contexto atual que já foi propagado pelo recordSpan
		Span httpSpan = telemetryManager.createSpan("http-request");
		telemetryManager.addSpanAttribute(httpSpan, "http.method", method);
		telemetryManager.addSpanAttribute(httpSpan, "http.url", target.toString());

		try (Scope httpScope = telemetryManager.activateSpan(httpSpan)) {
			var data = record.value().toString();
			log.info(data);

			ClassicHttpRequest request = getRequested(record);
            var modifiedUrl = request.getUri().toString();
			telemetryManager.addSpanAttribute(httpSpan, "http.request_uri", modifiedUrl);

			HttpCoreContext coreContext = HttpCoreContext.create();

			try (ClassicHttpResponse response = httpRequester.execute(target, request, timeout, coreContext)) {
				int statusCode = response.getCode();
				telemetryManager.addSpanAttribute(httpSpan, "http.status_code", statusCode);

				log.info(modifiedUrl + " --> " + statusCode);

				String responseBody = "";
				if (statusCode != 204) {
					responseBody = EntityUtils.toString(response.getEntity());
					log.info(responseBody);
                    httpSpan.addEvent(responseBody);
					telemetryManager.addSpanAttribute(httpSpan, "http.response_size", responseBody.length());
				}

				log.info("==============");

				Properties prop = new Properties();
				prop.load(HttpSinkTask.class.getClassLoader().getResourceAsStream("config.properties"));
				log.info(prop.getProperty("service.framework.name"));

				// Verificar se a requisição foi bem-sucedida
				if (statusCode >= 400) {
					telemetryManager.addSpanAttribute(httpSpan, "http.error", true);
					httpSpan.setStatus(StatusCode.ERROR, "HTTP " + statusCode);
					throw new ConnectException("Falha na requisição HTTP: " + statusCode);
				} else {
					telemetryManager.addSpanAttribute(httpSpan, "http.error", false);
					httpSpan.setStatus(StatusCode.OK);
				}

			} catch (IOException | HttpException e) {
				telemetryManager.recordException(httpSpan, e);
				telemetryManager.addSpanAttribute(httpSpan, "http.error", true);
				httpSpan.setStatus(StatusCode.ERROR, e.getMessage());
				log.error("Erro na requisição HTTP: {}", e.getMessage());
				throw e;
			}
		} finally {
			telemetryManager.finishSpan(httpSpan);
		}
	}

	private ClassicHttpRequest getRequested(final SinkRecord record) throws Exception {
	    String key = record.key() != null ? record.key().toString() : null;

        var content = parseRecord(record, converter);
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

    private Object parseRecord(SinkRecord record, String converter) {
        Object data = null;

        if (converter.contains("JsonConverter")) {
            // JSON normalmente vem como Struct ou Map
            Object value = record.value();
            if (value instanceof String) {
                try {
                    JSONObject json = new JSONObject((String) value);
                    if (json.has("payload")) {
                        data = json.get("payload");
                    } else {
                        data = json;
                    }
                } catch (Exception e) {
                    log.warn("Erro ao processar String como JSON: {}", e.getMessage());
                    data = value; // fallback para string original
                }
            } else if (value instanceof Struct) {
                // Kafka Connect Struct - o caso mais comum com JsonConverter
                Struct struct = (Struct) value;
                try {
                    // Converter Struct para Map para uso posterior
                    Map<String, Object> structMap = convertStructToMap(struct);

                    // Tentar pegar o campo "payload" primeiro
                    if (struct.schema().field("payload") != null) {
                        Object payload = struct.get("payload");
                        if (payload instanceof Struct) {
                            data = convertStructToMap((Struct) payload);
                        } else {
                            data = payload;
                        }
                    } else {
                        // Se não houver campo "payload", usar a struct convertida
                        data = structMap;
                    }
                } catch (Exception e) {
                    log.warn("Erro ao processar Struct: {}", e.getMessage());
                    data = convertStructToMap(struct); // fallback
                }
            } else if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> mapValue = (Map<String, Object>) value;
                data = mapValue.get("payload") != null ? mapValue.get("payload") : mapValue;
            } else {
                data = value; // fallback
            }
        } else if (converter.contains("AvroConverter")) {
            if (record.value() instanceof GenericRecord) {
                GenericRecord avroRecord = (GenericRecord) record.value();
                // Pega um campo específico do Avro
                Object payload = avroRecord.get("payload");
                data = payload != null ? payload.toString() : avroRecord.toString();
            } else {
                throw new IllegalArgumentException("Valor não é um GenericRecord para AvroConverter");
            }
        } else if (converter.contains("StringConverter")) {
            data = record.value().toString();
        } else {
            throw new IllegalArgumentException("Converter não suportado: " + converter);
        }

        return data;
    }

    // Método auxiliar para converter Struct em Map
    private Map<String, Object> convertStructToMap(Struct struct) {
        Map<String, Object> map = new HashMap<>();
        struct.schema().fields().forEach(field -> {
            try {
                Object value = struct.get(field);
                if (value instanceof Struct) {
                    // Recursivamente converter Structs aninhadas
                    map.put(field.name(), convertStructToMap((Struct) value));
                } else {
                    map.put(field.name(), value);
                }
            } catch (Exception e) {
                log.warn("Erro ao converter campo '{}' da Struct: {}", field.name(), e.getMessage());
            }
        });
        return map;
    }
}
