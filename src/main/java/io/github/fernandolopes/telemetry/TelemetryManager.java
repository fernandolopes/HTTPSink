package io.github.fernandolopes.telemetry;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.LogRecordProcessor;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

public class TelemetryManager {

    private static final Logger log = LoggerFactory.getLogger(TelemetryManager.class);
    private static TelemetryManager instance;
    private OpenTelemetry openTelemetry;
    private Tracer tracer;
    private boolean initialized = false;

    private TelemetryManager() {}

    public static synchronized TelemetryManager getInstance() {
        if (instance == null) {
            instance = new TelemetryManager();
        }
        return instance;
    }

    public void initialize(Map<String, String> config) {
        if (initialized) {
            return;
        }

        try {
            String serviceName = getConfigValue(config, "OTEL_SERVICE_NAME", "kafka-connect-http-sink");
            String serviceVersion = getConfigValue(config, "OTEL_SERVICE_VERSION", "0.0.33");
            String otlpEndpoint = getConfigValue(config, "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317");
            String environment = getConfigValue(config, "OTEL_RESOURCE_ATTRIBUTES", "production");

            log.info("Inicializando OpenTelemetry - Service: {}, Version: {}, Endpoint: {}",
                    serviceName, serviceVersion, otlpEndpoint);

            // Verificar se o GlobalOpenTelemetry já foi configurado
            if (GlobalOpenTelemetry.get() != OpenTelemetry.noop()) {
                log.info("OpenTelemetry já foi inicializado globalmente, reutilizando instância existente");
                openTelemetry = GlobalOpenTelemetry.get();
                tracer = openTelemetry.getTracer("kafka-connect-http-sink", serviceVersion);
                initialized = true;
                return;
            }

            // Criar resource
            Resource resource = Resource.getDefault()
                    .merge(Resource.builder()
                            .put(ResourceAttributes.SERVICE_NAME, serviceName)
                            .put(ResourceAttributes.SERVICE_VERSION, serviceVersion)
                            .put(ResourceAttributes.DEPLOYMENT_ENVIRONMENT, environment)
                            .build());

            // Configurar traces
            SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                    .addSpanProcessor(BatchSpanProcessor.builder(
                            OtlpGrpcSpanExporter.builder()
                                    .setEndpoint(otlpEndpoint)
                                    .build())
                            .setMaxExportBatchSize(512)
                            .setScheduleDelay(Duration.ofMillis(500))
                            .build())
                    .setResource(resource)
                    .build();

            // Configurar métricas
            SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                    .registerMetricReader(PeriodicMetricReader.builder(
                            OtlpGrpcMetricExporter.builder()
                                    .setEndpoint(otlpEndpoint)
                                    .build())
                            .setInterval(Duration.ofSeconds(30))
                            .build())
                    .setResource(resource)
                    .build();

            // Configurar logs
            LogRecordProcessor logProcessor = BatchLogRecordProcessor.builder(
                    OtlpGrpcLogRecordExporter.builder()
                            .setEndpoint(otlpEndpoint)
                            .build())
                    .build();

            SdkLoggerProvider loggerProvider = SdkLoggerProvider.builder()
                    .addLogRecordProcessor(logProcessor)
                    .setResource(resource)
                    .build();

            // Criar OpenTelemetry SDK
            openTelemetry = OpenTelemetrySdk.builder()
                    .setTracerProvider(tracerProvider)
                    .setMeterProvider(meterProvider)
                    .setLoggerProvider(loggerProvider)
                    .build();

            // Registrar globalmente se ainda não foi feito
            if (GlobalOpenTelemetry.get() == OpenTelemetry.noop()) {
                GlobalOpenTelemetry.set(openTelemetry);
            }

            tracer = openTelemetry.getTracer("kafka-connect-http-sink", serviceVersion);
            initialized = true;

            log.info("OpenTelemetry inicializado com sucesso!");

        } catch (Exception e) {
            log.error("Erro ao inicializar OpenTelemetry: {}", e.getMessage(), e);
            // Fallback para noop
            openTelemetry = OpenTelemetry.noop();
            tracer = openTelemetry.getTracer("kafka-connect-http-sink");
            initialized = true;
        }
    }

    public Span createSpan(String operationName) {
        return createSpan(operationName, null);
    }

    public Span createSpan(String operationName, Span parent) {
        if (!initialized) {
            log.warn("TelemetryManager não foi inicializado");
            return Span.getInvalid();
        }

        Context context = parent != null ? Context.current().with(parent) : Context.current();
        return tracer.spanBuilder(operationName)
                .setParent(context)
                .startSpan();
    }

    public Scope activateSpan(Span span) {
        return span.makeCurrent();
    }

    public void addSpanAttribute(Span span, String key, String value) {
        if (span != null && value != null) {
            span.setAttribute(key, value);
        }
    }

    public void addSpanAttribute(Span span, String key, long value) {
        if (span != null) {
            span.setAttribute(key, value);
        }
    }

    public void addSpanAttribute(Span span, String key, boolean value) {
        if (span != null) {
            span.setAttribute(key, value);
        }
    }

    public void recordException(Span span, Throwable throwable) {
        if (span != null && throwable != null) {
            span.recordException(throwable);
        }
    }

    public void finishSpan(Span span) {
        if (span != null) {
            span.end();
        }
    }

    private String getConfigValue(Map<String, String> config, String key, String defaultValue) {
        // Primeiro tenta pegar do config do connector
        String value = config.get(key.toLowerCase().replace("_", "."));
        if (value != null) {
            return value;
        }

        // Depois tenta pegar das variáveis de ambiente
        value = System.getenv(key);
        if (value != null) {
            return value;
        }

        // Por último, usa o valor padrão
        return defaultValue;
    }

    public boolean isInitialized() {
        return initialized;
    }
}
