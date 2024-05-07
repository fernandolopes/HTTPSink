package io.github.fernandolopes.core;

import org.apache.kafka.connect.header.Headers;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
//import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
//import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
//import io.opentelemetry.context.propagation.ContextPropagators;
//import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
//import io.opentelemetry.sdk.logs.SdkLoggerProvider;
//import io.opentelemetry.sdk.logs.export.LogRecordExporter;
//import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
//import io.opentelemetry.sdk.metrics.SdkMeterProvider;
//import io.opentelemetry.sdk.metrics.export.MetricExporter;
//import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
//import io.opentelemetry.sdk.resources.Resource;
//import io.opentelemetry.sdk.trace.SdkTracerProvider;
//import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
//import io.opentelemetry.sdk.trace.export.SpanExporter;
//import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
//import io.opentelemetry.exporter.otlp.http.logs.OtlpHttpLogRecordExporter;
//import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
//import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
//import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
//import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
//import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;

public class TelemetryConfig {
	
	public static OpenTelemetry initOpenTelemetry() {
		try {
			return AutoConfiguredOpenTelemetrySdk.initialize().getOpenTelemetrySdk();
		}
		catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
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
