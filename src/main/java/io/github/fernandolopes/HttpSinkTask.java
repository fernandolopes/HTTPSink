package io.github.fernandolopes;

import java.net.ConnectException;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpSinkTask extends SinkTask {

	private static final Logger log = LoggerFactory.getLogger(HttpSinkTask.class);
	private HttpSender httpSender;
	private TelemetryManager telemetryManager;
	private HttpSinkConfigManager configManager;
	private ErrantRecordReporter reporter;
	private int remainingRetries;
	private int maxRetries;
	private int retryBackoffMs;

	@Override
	public String version() {
		return new HttpSinkConnect().version();
	}

	@Override
	public void start(Map<String, String> props) {
		log.info("Starting HttpSinkTask...");
		configManager = new HttpSinkConfigManager(props);
		telemetryManager = new TelemetryManager(configManager.getOpenTelemetry());
		try {
			httpSender = new HttpSender(configManager);
		} catch (Exception e) {
			e.printStackTrace();
		}

		remainingRetries = configManager.getMaxRetries();
		maxRetries = remainingRetries;
		retryBackoffMs = configManager.getRetryBackoffMs();

		if (context != null) {
			try {
				reporter = context.errantRecordReporter();
			} catch (NoSuchMethodError | NoClassDefFoundError e) {
				log.warn("Unable to instantiate ErrantRecordReporter.");
				reporter = null;
			}
		}
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		if (records.isEmpty()) {
			return;
		}

		for (SinkRecord record : records) {
			try {
				telemetryManager.processRecord(record, span -> {
					httpSender.send(record, span);
				});
			} catch (ConnectException e) {
				handleRetryableException(record, e);
			} catch (Exception e) {
				log.error("Error processing record: {}", e.getMessage());
				throw new RetriableException("Failed to send message", e);
			}
		}
	}

	private void handleRetryableException(SinkRecord record, ConnectException e) {
		log.error("Controlled connectivity failure: {}", e.getMessage());
		if (remainingRetries > 0) {
			remainingRetries--;
			context.timeout(retryBackoffMs);
			throw new RetriableException("Failed to send message", e);
		}
		remainingRetries = maxRetries;
		if (reporter != null) {
			log.warn("Sending record to the DLQ");
			reporter.report(record, e);
		}
	}

	@Override
	public void stop() {
		log.info("Stopping HttpSinkTask...");
		httpSender.close();
	}
}
