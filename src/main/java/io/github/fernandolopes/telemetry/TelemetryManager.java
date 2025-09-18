package io.github.fernandolopes.telemetry;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
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
    private boolean telemetryEnabled = false;

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
            String serviceVersion = getConfigValue(config, "OTEL_SERVICE_VERSION", "0.0.36");
            String otlpEndpoint = getConfigValue(config, "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317");
            String environment = getConfigValue(config, "OTEL_RESOURCE_ATTRIBUTES", "development");

            log.info("Inicializando OpenTelemetry 1.36.0");
            log.info("Service: {}, Version: {}", serviceName, serviceVersion);
            log.info("OTLP Endpoint: {}", otlpEndpoint);
            log.info("Environment: {}", environment);

            // Verificar se já existe uma instância global
            if (GlobalOpenTelemetry.get() != OpenTelemetry.noop()) {
                log.info("Reutilizando instância OpenTelemetry existente");
                openTelemetry = GlobalOpenTelemetry.get();
                tracer = openTelemetry.getTracer("kafka-connect-http-sink", serviceVersion);
                telemetryEnabled = true;
                initialized = true;
                return;
            }

            // Inicializar OpenTelemetry com OTLP
            if (initializeWithOtlp(serviceName, serviceVersion, otlpEndpoint, environment)) {
                telemetryEnabled = true;
                log.info("✅ OpenTelemetry inicializado com sucesso! Traces serão enviados para: {}", otlpEndpoint);
            } else {
                log.warn("⚠️ Falhou ao inicializar OTLP, usando modo básico");
                initializeBasicSdk(serviceName, serviceVersion, environment);
            }

        } catch (Exception e) {
            log.error("❌ Erro crítico ao inicializar OpenTelemetry: {}", e.getMessage(), e);
            initializeNoopTelemetry();
        }
    }

    private boolean initializeWithOtlp(String serviceName, String serviceVersion, String otlpEndpoint, String environment) {
        try {
            log.info("🚀 Criando OTLP gRPC Exporter para: {}", otlpEndpoint);

            // Criar Resource detalhado
            Resource resource = Resource.getDefault()
                    .merge(Resource.create(Attributes.of(
                            ResourceAttributes.SERVICE_NAME, serviceName,
                            ResourceAttributes.SERVICE_VERSION, serviceVersion,
                            ResourceAttributes.DEPLOYMENT_ENVIRONMENT, environment,
                            ResourceAttributes.SERVICE_INSTANCE_ID, java.util.UUID.randomUUID().toString()
                    )));

            // Configurar OTLP Exporter com timeout e retry
            OtlpGrpcSpanExporter spanExporter = OtlpGrpcSpanExporter.builder()
                    .setEndpoint(otlpEndpoint)
                    .setTimeout(Duration.ofSeconds(30))
                    .build();

            // Configurar BatchSpanProcessor otimizado
            BatchSpanProcessor spanProcessor = BatchSpanProcessor.builder(spanExporter)
                    .setMaxExportBatchSize(512)          // Aumentar batch size
                    .setScheduleDelay(Duration.ofSeconds(5))   // Exportar a cada 5 segundos
                    .build();

            // Criar TracerProvider
            SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                    .addSpanProcessor(spanProcessor)
                    .setResource(resource)
                    .build();

            // Tentar registrar globalmente
            try {
                openTelemetry = OpenTelemetrySdk.builder()
                        .setTracerProvider(tracerProvider)
                        .buildAndRegisterGlobal();

                log.info("✅ OpenTelemetry registrado globalmente");
            } catch (IllegalStateException e) {
                log.info("ℹ️ Criando instância local (global já existe)");
                openTelemetry = OpenTelemetrySdk.builder()
                        .setTracerProvider(tracerProvider)
                        .build();
            }

            tracer = openTelemetry.getTracer("kafka-connect-http-sink", serviceVersion);
            initialized = true;

            // Testar com um span simples
            testTelemetryConnection();

            return true;

        } catch (Exception e) {
            log.warn("❌ Falha ao inicializar OTLP: {}", e.getMessage());
            return false;
        }
    }

    private void testTelemetryConnection() {
        try {
            log.info("🧪 Testando conexão com telemetria...");
            Span testSpan = tracer.spanBuilder("telemetry-test")
                    .setAttribute("test.component", "kafka-connect-http-sink")
                    .setAttribute("test.timestamp", System.currentTimeMillis())
                    .startSpan();

            try (Scope scope = testSpan.makeCurrent()) {
                testSpan.addEvent("Telemetria inicializada com sucesso");
                Thread.sleep(10); // Pequena pausa para simular trabalho
            } finally {
                testSpan.end();
            }

            log.info("✅ Span de teste criado e enviado!");
        } catch (Exception e) {
            log.warn("⚠️ Erro no teste de telemetria: {}", e.getMessage());
        }
    }

    private void initializeBasicSdk(String serviceName, String serviceVersion, String environment) {
        try {
            log.info("📋 Inicializando SDK básico (sem OTLP export)");

            Resource resource = Resource.getDefault()
                    .merge(Resource.create(Attributes.of(
                            ResourceAttributes.SERVICE_NAME, serviceName,
                            ResourceAttributes.SERVICE_VERSION, serviceVersion,
                            ResourceAttributes.DEPLOYMENT_ENVIRONMENT, environment
                    )));

            SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                    .setResource(resource)
                    .build();

            try {
                openTelemetry = OpenTelemetrySdk.builder()
                        .setTracerProvider(tracerProvider)
                        .buildAndRegisterGlobal();
            } catch (IllegalStateException e) {
                openTelemetry = OpenTelemetrySdk.builder()
                        .setTracerProvider(tracerProvider)
                        .build();
            }

            tracer = openTelemetry.getTracer("kafka-connect-http-sink", serviceVersion);
            telemetryEnabled = true;
            initialized = true;

            log.info("✅ SDK básico inicializado (traces em memória)");
        } catch (Exception e) {
            log.error("❌ Erro ao inicializar SDK básico: {}", e.getMessage());
            initializeNoopTelemetry();
        }
    }

    private void initializeNoopTelemetry() {
        log.warn("🔕 Inicializando em modo noop (sem telemetria)");
        openTelemetry = OpenTelemetry.noop();
        tracer = openTelemetry.getTracer("kafka-connect-http-sink");
        initialized = true;
        telemetryEnabled = false;
    }

    public Span createSpan(String operationName) {
        return createSpan(operationName, null);
    }

    public Span createSpan(String operationName, Span parent) {
        if (!initialized) {
            log.warn("TelemetryManager não inicializado, retornando span inválido");
            return Span.getInvalid();
        }

        if (!telemetryEnabled) {
            return Span.getInvalid();
        }

        try {
            Context context = parent != null ? Context.current().with(parent) : Context.current();
            Span span = tracer.spanBuilder(operationName)
                    .setParent(context)
                    .startSpan();

            // Adicionar informações básicas automaticamente
            span.setAttribute("component", "kafka-connect-http-sink");
            span.setAttribute("span.kind", "internal");

            return span;
        } catch (Exception e) {
            log.debug("Erro ao criar span '{}': {}", operationName, e.getMessage());
            return Span.getInvalid();
        }
    }

    public Scope activateSpan(Span span) {
        if (!telemetryEnabled || span == null || !span.isRecording()) {
            return () -> {}; // Scope vazio
        }

        try {
            return span.makeCurrent();
        } catch (Exception e) {
            log.debug("Erro ao ativar span: {}", e.getMessage());
            return () -> {}; // Scope vazio
        }
    }

    public void addSpanAttribute(Span span, String key, String value) {
        if (span != null && value != null && telemetryEnabled && span.isRecording()) {
            try {
                span.setAttribute(key, value);
            } catch (Exception e) {
                log.debug("Erro ao adicionar atributo string: {}", e.getMessage());
            }
        }
    }

    public void addSpanAttribute(Span span, String key, long value) {
        if (span != null && telemetryEnabled && span.isRecording()) {
            try {
                span.setAttribute(key, value);
            } catch (Exception e) {
                log.debug("Erro ao adicionar atributo long: {}", e.getMessage());
            }
        }
    }

    public void addSpanAttribute(Span span, String key, boolean value) {
        if (span != null && telemetryEnabled && span.isRecording()) {
            try {
                span.setAttribute(key, value);
            } catch (Exception e) {
                log.debug("Erro ao adicionar atributo boolean: {}", e.getMessage());
            }
        }
    }

    public void recordException(Span span, Throwable throwable) {
        if (span != null && throwable != null && telemetryEnabled && span.isRecording()) {
            try {
                span.recordException(throwable);
            } catch (Exception e) {
                log.debug("Erro ao gravar exceção: {}", e.getMessage());
            }
        }
    }

    public void finishSpan(Span span) {
        if (span != null && telemetryEnabled && span.isRecording()) {
            try {
                span.end();
            } catch (Exception e) {
                log.debug("Erro ao finalizar span: {}", e.getMessage());
            }
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

    public boolean isTelemetryEnabled() {
        return telemetryEnabled;
    }
}
