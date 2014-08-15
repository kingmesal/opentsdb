package net.opentsdb.tools.load;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.opentsdb.core.TSDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jscott
 */
class ImportMetrics {

	private static final Logger LOG = LoggerFactory.getLogger(ImportMetrics.class);

	private static final String UNKNOWN_HOST = "unknown";

	private static final String HOSTNAME_KEY = "host.name";
	private static final String JOBID_KEY = "job.id";
	private static final String THREADID_KEY = "thread.id";
	private static final String IMPORTMODE_KEY = "import.mode";

	private static final String AVG_DPS_PER_SEC_KEY = "tsdb.import.avg.dps.1s";
	private static final String AVG_PUTS_PER_SEC_KEY = "tsdb.import.avg.puts.1s";
	private static final String TOTAL_DPS_KEY = "tsdb.import.total.dps";
	private static final String TOTAL_PUTS_KEY = "tsdb.import.total.puts";
	private static final String TOTAL_SECONDS_KEY = "tsdb.import.total.secs";
	private static final String AVG_DPS_PER_SEC_POST_KEY = "tsdb.import.postflush.avg.dps.1s";
	private static final String AVG_PUTS_PER_SEC_POST_KEY = "tsdb.import.postflush.avg.puts.1s";
	private static final String TOTAL_SECONDS_POST_KEY = "tsdb.import.postflush.total.secs";
	private static final String HOST_NAME;
	private static final String[] METRICS = {
		AVG_DPS_PER_SEC_KEY, AVG_DPS_PER_SEC_POST_KEY, AVG_PUTS_PER_SEC_KEY,
		AVG_PUTS_PER_SEC_POST_KEY, TOTAL_SECONDS_POST_KEY, TOTAL_SECONDS_KEY, TOTAL_DPS_KEY, TOTAL_PUTS_KEY
	};

	static {
		for (String metric : METRICS) {
			LOG.info("The following metric.name will be stored: {}", metric);
		}

		String hn = UNKNOWN_HOST;
		try {
			hn = InetAddress.getLocalHost().getHostName();
		}
		catch (UnknownHostException ex) {
		}
		HOST_NAME = hn;
	}

	private final TSDB tsdb;
	private final String jobId;
	private final List<ImportResult> results = new ArrayList<ImportResult>();
	private final ImportMode importMode;
	private long startTime = Long.MIN_VALUE;
	private long totalMillis = 0;
	private long totalDatapoints = 0;
	private long totalPuts = 0;

	public ImportMetrics(final ImportMode importMode, final TSDB tsdb, final String jobId) {
		this.importMode = importMode;
		this.tsdb = tsdb;
		this.jobId = jobId;
	}

	/**
	 * Creates metrics in OpenTSDB describing the following performance of an import job. The
	 * timestamp for the metrics produced are based off of the end time of each import result.
	 *
	 * tsdb.import.avg.dps.1s: This is the average number of datapoints per second
	 * tsdb.import.avg.puts.1s: This is the average number of puts per second
	 * tsdb.import.total.secs: This is the total number of seconds this job ran
	 * tsdb.import.total.dps: This is the total number of data points this job put
	 * tsdb.import.total.puts: This is the total number of puts
	 *
	 * tags: host.name=? job.id=? import.mode=? thread.id=?
	 *
	 * @param result Completely populated result.
	 */
	public void add(final ImportResult result) {
		results.add(result);
		if (startTime == Long.MIN_VALUE) {
			startTime = result.getStart();
		}
		totalMillis += result.getTotalMillis();
		totalDatapoints += result.getTotalDatapoints();
		totalPuts += result.getTotalPuts();

		Map<String, String> tags = new HashMap<String, String>();
		tags.put(HOSTNAME_KEY, HOST_NAME);
		tags.put(JOBID_KEY, jobId);
		tags.put(IMPORTMODE_KEY, importMode.toString());
		tags.put(THREADID_KEY, String.valueOf(result.getThreadId()));

		long endTime = result.getEnd();
		tsdb.addPoint(AVG_DPS_PER_SEC_KEY, endTime, result.getTotalDatapoints() / result.getTotalSeconds(), tags);
		tsdb.addPoint(AVG_PUTS_PER_SEC_KEY, endTime, result.getTotalPuts() / result.getTotalSeconds(), tags);
		tsdb.addPoint(TOTAL_SECONDS_KEY, endTime, result.getTotalSeconds(), tags);
		tsdb.addPoint(TOTAL_DPS_KEY, endTime, result.getTotalDatapoints(), tags);
		tsdb.addPoint(TOTAL_PUTS_KEY, endTime, result.getTotalPuts(), tags);
	}

	/**
	 * Creates metrics in OpenTSDB describing the following performance of an import job. The
	 * timestamps for these metrics are based off the time this method is called.
	 *
	 * tsdb.import.postflush.avg.dps.1s: This is the average number of datapoints per second
	 * tsdb.import.postflush.avg.puts.1s: This is the average number of puts per second
	 * tsdb.import.postflush.total.secs: This is the total number of seconds this job ran
	 *
	 * tags: host.name=? job.id=? import.mode=?
	 */
	public void complete() {
		final long endTime = System.currentTimeMillis();
		long totalRuntimeSeconds = (endTime - startTime) / 1000;

		LOG.info("Job ID: {} - Total import results: {}", jobId, results.size());
		LOG.info("Thread\tMetric\tYear\tStart\tEnd\tFlushed\tTotal Millis\tTotal Seconds\tPuts/sec");
		for (ImportResult result : results) {
			LOG.info("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}", result.getThreadId(), result.getMetric(), result.getYear(), result.getStart(), result.getEnd(), result.getTotalMillis(), result.getTotalSeconds(), result.getTotalPuts() / result.getTotalSeconds());
		}
		Map<String, String> tags = new HashMap<String, String>();
		tags.put(HOSTNAME_KEY, HOST_NAME);
		tags.put(JOBID_KEY, jobId);
		tags.put(IMPORTMODE_KEY, importMode.toString());

		if (totalMillis != 0 && totalDatapoints != 0) {
			LOG.info("Total puts:\t{}\t- Total datapoints:\t{}\t- Total time:\t{}", totalPuts, totalDatapoints, totalRuntimeSeconds);

			long avgDpsPerSecPostFlush = totalDatapoints / totalRuntimeSeconds;
			tsdb.addPoint(AVG_DPS_PER_SEC_POST_KEY, endTime, avgDpsPerSecPostFlush, tags);
			LOG.info("Average datapoints per second (post flush):\t{}", avgDpsPerSecPostFlush);

			long avgPutsPerSecPostFlush = totalPuts / totalRuntimeSeconds;
			tsdb.addPoint(AVG_PUTS_PER_SEC_POST_KEY, endTime, avgPutsPerSecPostFlush, tags);
			LOG.info("Average puts per second (post flush):\t{}", avgPutsPerSecPostFlush);
		}
		else {
			LOG.warn("There was a problem, nothing was recorded");
		}

		tsdb.addPoint(TOTAL_SECONDS_POST_KEY, endTime, totalRuntimeSeconds, tags);
	}

}
