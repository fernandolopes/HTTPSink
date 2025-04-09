package io.github.fernandolopes.core;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.kafka.connect.header.Headers;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.logging.LoggingSpanExporter;
import io.opentelemetry.exporter.otlp.http.logs.OtlpHttpLogRecordExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;

public class TelemetryConfig {
	
	public static OpenTelemetry initOpenTelemetry() {
		try {
        
			var service = "connect-http-sink";//System.getenv("OTEL_SERVICE_NAME");
			
			if (service == null)
				return null;
			
			var resource = Resource.getDefault()
			.toBuilder()
			.put(ResourceAttributes.SERVICE_NAME, service)
			.put(ResourceAttributes.SERVICE_VERSION, "1.0.0")
			.put(ResourceAttributes.SERVICE_NAMESPACE, "io.github.fernandolopes")
			.build();
	        
	        var otelHeader = System.getenv("OTEL_EXPORTER_OTLP_HEADERS");
	        Supplier<Map<String, String>> mapSupplier = null;
	        
	        if(otelHeader != null && !otelHeader.isEmpty()) {
	        	
	        	mapSupplier = new Supplier<Map<String, String>>() {
	                @Override
	                public Map<String, String> get() {
	                    String[] parts = otelHeader.split("=", 2);
	                    Map<String, String> map = new HashMap<>();
	                    if (parts.length == 2) {
	                        map.put(parts[0], parts[1]);
	                    } else {
	                        System.out.println("A string de entrada não está no formato esperado.");
	                    }
	                    return map;
	                }
	            };
	        }
			var endpoint = "http://localhost:4318";//System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT");
			System.out.println("endpoint otel" + endpoint);
			
			SpanExporter spanExporter = null;
			MetricExporter metricExporter = null;
			LogRecordExporter logExporter = null;
			
			if (!endpoint.contains("4317")) {
				var sExporter = OtlpHttpSpanExporter.builder();
				if(otelHeader != null && !otelHeader.isEmpty())
					sExporter.setHeaders(mapSupplier);
		        spanExporter = sExporter.setEndpoint(endpoint + "/v1/traces")
		        		.build();

				var mExpoter = OtlpHttpMetricExporter.builder();
				if(otelHeader != null && !otelHeader.isEmpty())
						mExpoter.setHeaders(mapSupplier);
				metricExporter = mExpoter.setEndpoint(endpoint + "/v1/metrics")
		        		.build();

				var lExporter = OtlpHttpLogRecordExporter.builder();
				if(otelHeader != null && !otelHeader.isEmpty())
					lExporter.setHeaders(mapSupplier);
				logExporter	= lExporter.setEndpoint(endpoint + "/v1/logs")
		        		.build();
			}
			else {
				var sExporter = OtlpGrpcSpanExporter.builder();
				if(otelHeader != null && !otelHeader.isEmpty())
					sExporter.setHeaders(mapSupplier);
		        spanExporter = sExporter.setEndpoint(endpoint + "/v1/traces")
		        		.build();

				var mExpoter = OtlpGrpcMetricExporter.builder();
				if(otelHeader != null && !otelHeader.isEmpty())
						mExpoter.setHeaders(mapSupplier);
				metricExporter = mExpoter.setEndpoint(endpoint + "/v1/metrics")
		        		.build();

				var lExporter = OtlpGrpcLogRecordExporter.builder();
				if(otelHeader != null && !otelHeader.isEmpty())
					lExporter.setHeaders(mapSupplier);
				logExporter	= lExporter.setEndpoint(endpoint + "/v1/logs")
		        		.build();
			}
			
	        
	        var loggerProvider = SdkLoggerProvider.builder()
	        		.addLogRecordProcessor(SimpleLogRecordProcessor.create(logExporter))
	    			.setResource(resource)
	        		.build();
	        
	        var metricProvider = SdkMeterProvider.builder()
	        		.setResource(resource)
	        		.registerMetricReader(PeriodicMetricReader.builder(metricExporter).build())
	        		.build();
	        
			LoggingSpanExporter loggingSpanExporter = LoggingSpanExporter.create();

	        var tracerProvider = SdkTracerProvider.builder()
	                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
					.addSpanProcessor(SimpleSpanProcessor.create(loggingSpanExporter))
	                .setResource(resource)
	                .build();
	        
	        
	        return OpenTelemetrySdk.builder()
	        		.setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
	        		.setTracerProvider(tracerProvider)
	        		.setMeterProvider(metricProvider)
	        		.setLoggerProvider(loggerProvider)
	        		.build();
		}
		catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}
		
	public static Span getContext(Headers headerList, Tracer tracer) {
		String traceparent = null;
		for(var e : headerList) {
			System.out.println("chave: " + e.key());
			if (e.key().equals("traceparent"))
				traceparent = (String) e.value(); 
		}
		System.out.println("trace current: " + traceparent);
		
	
		SpanBuilder span = tracer.spanBuilder("root span name").setSpanKind(SpanKind.CONSUMER);

		if (traceparent != null) {
			String[] ids = Utils.extractIds(traceparent);
			
			SpanContext remoteContext = SpanContext.createFromRemoteParent(
					ids[0],
					ids[1],
					TraceFlags.getSampled(),
					TraceState.getDefault());

			span.setParent(Context.current().with(Span.wrap(remoteContext)));
		}

		return span.startSpan();
		
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
