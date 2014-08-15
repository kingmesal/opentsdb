package net.opentsdb.tools.load;

import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jscott
 */
class ImportResult {

	private static final Logger LOG = LoggerFactory.getLogger(ImportResult.class);

	private final int threadId;
	private final String metricName;
	private final int year;
	private final long start;
	private final long end;
	private final long totalMillis;
	private final long totalSeconds;
	private final long totalPuts;
	private final long totalDatapoints;

	private static final AtomicInteger globalThreadIds = new AtomicInteger(0);
	private static final ThreadLocal<Integer> localThreadId = new ThreadLocal<Integer>() {
		@Override
		protected synchronized Integer initialValue() {
			LOG.info("Getting next thread id");
			return globalThreadIds.getAndIncrement();
		}
	};

	public ImportResult(final String metricName, final long totalPuts, final long totalDatapoints, final int year, final long start, final long end) {
		this.threadId = localThreadId.get();
		this.totalPuts = totalPuts;
		this.totalDatapoints = totalDatapoints;
		this.metricName = metricName;
		this.year = year;
		this.start = start;
		this.end = end;
		this.totalMillis = end - start;
		this.totalSeconds = totalMillis / 1000;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("threadId: ").append(threadId).append(", ");
		sb.append("metric: ").append(metricName).append(", ");
		sb.append("year: ").append(year).append(", ");
		sb.append("start: ").append(start).append(", ");
		sb.append("end: ").append(end).append(", ");
		sb.append("totalMillis: ").append(totalMillis).append(", ");
		sb.append("totalPuts: ").append(totalPuts).append(", ");
		sb.append("totalDatapoints: ").append(totalDatapoints).append(", ");
		sb.append("totalSeconds: ").append(totalSeconds);
		return sb.toString();
	}

	public String getMetric() {
		return metricName;
	}

	public int getYear() {
		return year;
	}

	public long getStart() {
		return start;
	}

	public long getEnd() {
		return end;
	}

	public long getTotalMillis() {
		return totalMillis;
	}

	public long getTotalSeconds() {
		return totalSeconds;
	}

	public long getTotalPuts() {
		return totalPuts;
	}

	public long getTotalDatapoints() {
		return totalDatapoints;
	}

	public int getThreadId() {
		return threadId;
	}

}
