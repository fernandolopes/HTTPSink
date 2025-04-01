package io.github.fernandolopes;

import io.github.fernandolopes.core.TelemetryConfig;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.apache.kafka.connect.sink.SinkRecord;

public class TelemetryManager {

	private final Tracer tracer;

	public TelemetryManager(OpenTelemetry openTelemetry) {
		this.tracer = openTelemetry.getTracer(HttpSinkTask.class.getName(), "1.0.0");
	}

	public void processRecord(SinkRecord record, TelemetryCallback callback) throws Exception {
		Span mainSpan = TelemetryConfig.getContext(record.headers());
		Context parentContext = Context.current().with(mainSpan);

		try (Scope scope = mainSpan.makeCurrent()) {
			Span span = tracer.spanBuilder("processRecord")
					.setParent(parentContext)
					.startSpan();

			span.setAttribute("kafka.topic", record.topic());
			span.setAttribute("kafka.partition", record.kafkaPartition());
			span.setAttribute("kafka.offset", record.kafkaOffset());

			callback.execute(span);

			span.end();
			if (mainSpan != null) {
				mainSpan.end();
			}
		}
	}

	@FunctionalInterface
	public interface TelemetryCallback {
		void execute(Span span) throws Exception;
	}
}
