package io.github.fernandolopes.core;
import org.apache.kafka.connect.header.Headers;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;

public class TelemetryConfig {
	
	public static OpenTelemetry initOpenTelemetry() {
		// Configurar o endpoint OTLP explicitamente
        System.setProperty("otel.exporter.otlp.endpoint", "http://elasticlogsfleetapm-stg.pmenos.com.br");
        System.setProperty("otel.exporter.otlp.headers", "Authorization=Bearer k+dbcPb0lpu+HsrxsTn2P1LUOoCsE9THTX7GKe3tsuE=");
        System.setProperty("otel.service.name", "http-sink-connector");
		//System.setProperty("otel.exporter.otlp.protocol", "http/protobuf");
        return AutoConfiguredOpenTelemetrySdk.initialize().getOpenTelemetrySdk();
	}
		
	public static Span getContext(Headers headerList) {
		String traceparent = null;
		for(var e : headerList) {
			System.out.println("chave: " + e.key());
			if (e.key().equals("traceparent"))
				traceparent = (String) e.value(); 
		}
		System.out.println("trace current: " + traceparent);
		
		if(traceparent == null)
			return GlobalOpenTelemetry.getTracer("")
			        .spanBuilder("root span name")
			        .setSpanKind(SpanKind.INTERNAL)
			        .setParent(Context.current())
			        .startSpan();

		String[] ids = Utils.extractIds(traceparent);
		
		SpanContext remoteContext = SpanContext.createFromRemoteParent(
				ids[0],
				ids[1],
                TraceFlags.getSampled(),
                TraceState.getDefault());
		
		return GlobalOpenTelemetry.getTracer("")
		        .spanBuilder("root span name")
		        .setSpanKind(SpanKind.INTERNAL)
		        .setParent(Context.current().with(Span.wrap(remoteContext)))
		        .startSpan();
		
	}
	
	public static void GenerateLogs() {
//		var loggerProvider = openTelemetry.getLogsBridge();
//		var logger = loggerProvider.get("example");
//
//        logger.logRecordBuilder()
//        .setSeverity(Severity.WARN)
//        .setBody("A log message from a custom appender without a span")
//        .setAttribute(AttributeKey.stringKey("key"), "value")
//        .emit();
	}
    
}
