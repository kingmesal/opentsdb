// This file is part of OpenTSDB.
// Copyright (C) 2014 The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tools.load;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.WritableDataPoints;
import net.opentsdb.tools.ArgP;
import net.opentsdb.tools.load.Sampler.SamplePoint;
import net.opentsdb.utils.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkImport {

	private static final Logger LOG = LoggerFactory.getLogger(BulkImport.class);

	private final Map<String, String> tags = new HashMap<String, String>();
	private final List<String> metricNames = new ArrayList<String>();
	private final Config config;
	private final ImportMode importMode;
	private final String jobId;

	private final Set<TSDB> tsdbInstances = new HashSet<TSDB>();
	private final ThreadLocal<TSDB> tsdbPool = new ThreadLocal<TSDB>() {
		@Override
		protected synchronized TSDB initialValue() {
			LOG.info("Creating new instance of TSDB for another Thread");
			TSDB tsdb = new TSDB(config);
			tsdbInstances.add(tsdb);
			return tsdb;
		}
	};

	private static ArgP buildUsage(String[] args) {
		final ArgP argp = new ArgP();
		argp.addOption("--action", "ACTION", "Used by the driver to perform different types of loading");
		argp.addOption("--jobId", "JOBID", "preferably an incremented job number which started from one (required)");
		argp.addOption("--flushInterval", "FLUSH", "Puts will be flushed at this rate in milliseconds (default 1000ms)");
		argp.addOption("--quorum", "QUORUM", "server[:port][,server[:port]]...");
		argp.addOption("--year", "YEAR", "Year to generate data for");
		argp.addOption("--importMode", "MODE", "batch (one hour per put) or single (single data point per put)");
		argp.addOption("--totalYears", "YEARS", "Number of years to generate data for");
		argp.addOption("--threads", "THREADS", "Total number of concurrent threads to run");
		argp.addOption("--path", "PATH", "Path in M7 the tsdb tables are located at");
		argp.addOption("--tags", "key:value", "key value separated by a colon");
		argp.addOption("--metric", "METRIC.NAME", "name of the metric to store data points for");
		argp.addOption("--multiMetric", "NUMBEROFMETRICS", "If specified a .# will be appended to the name of the metric, with as many numbers as specified, e.g. 2 will generate two tags with .1 and .2 appended to the value");
		argp.parse(args);
		return argp;
	}

	private static void validateArgs(ArgP argp, String jobId, String importMode, String year) {
		boolean failed = false;
		if (jobId == null || jobId.trim().isEmpty()) {
			System.out.println("jobId must be defined! See Usage!\n");
			failed = true;
		}

		if (importMode == null || importMode.trim().isEmpty() || (!"batch".equalsIgnoreCase(importMode) && !"single".equalsIgnoreCase(importMode))) {
			System.out.println("importMode must be defined! See Usage!\n");
			failed = true;
		}

		if (year == null) {
			System.out.println("This application will generate N years worth of data for each metric generated with a given tag (e.g. 3 years x 10 metrics = 30 years of data");
			failed = true;
		}

		if (failed) {
			System.out.print(argp.usage());
			System.exit(1);
		}
	}

	public static void main(String[] args) throws Exception {
		ArgP argp = buildUsage(args);

		String jobId = argp.get("--jobId");
		String method = argp.get("--importMode");
		String yearValue = argp.get("--year");

		validateArgs(argp, jobId, method, yearValue);

		ImportMode mode = ImportMode.batch.toString().equalsIgnoreCase(method) ? ImportMode.batch : ImportMode.single;
		int year = Integer.parseInt(yearValue);
		int totalYears = Integer.parseInt(argp.get("--totalYears", "1"));
		int threads = Integer.parseInt(argp.get("--threads", "1"));
		int flushInterval = Integer.parseInt(argp.get("--flushInterval", "1000"));
		String quorum = argp.get("--quorum", "localhost:5181");
		String path = argp.get("--path", "/user/jscott/tables");
		String tags = argp.get("--tags", "location:here");
		String metric = argp.get("--metric", "blue.pill");
		int metricCount = argp.has("--multiMetric") ? Integer.parseInt(argp.get("--multiMetric")) : 1;

		LOG.info("Writing data for year: {}, quorum: {}, table path: {}, batch mode: {}", year, quorum, path, method);
		BulkImport batch = new BulkImport(jobId, mode, quorum, path, flushInterval, metric, metricCount, tags);
		batch.run(year, totalYears, threads);
	}

	private void loadMetrics(String metric, int count) {
		for (int i = 0; i < count; i++) {
			String metricName = count == 1 ? metric : metric + "." + i;
			metricNames.add(metricName);
		}
	}

	public BulkImport(final String jobId, final ImportMode importMode, final String quorum, final String tablesBasePath, final int flushInterval, final String metric, final int metricCount, final String tag) throws IOException {
		Preconditions.checkNotNull(jobId, "jobId cannot be null or empty");
		Preconditions.checkNotNull(metric, "metric cannot be null or empty");
		Preconditions.checkNotNull(metricCount > 0, "metric must be greater than zero");
		Preconditions.checkNotNull(tags, "tags cannot be null or empty");
		Preconditions.checkNotNull(quorum, "quorum must be specified");
		Preconditions.checkNotNull(tablesBasePath, "path to tsdb tables must be specified");

		this.jobId = jobId;
		this.importMode = importMode;
		loadMetrics(metric, metricCount);
		String[] kv = tag.split(":");
		tags.put(kv[0], kv[1]);

		config = new Config(false);
		config.setAutoMetric(true);
		config.overrideConfig("tsd.storage.hbase.data_table", tablesBasePath + "/tsdb");
		config.overrideConfig("tsd.storage.hbase.uid_table", tablesBasePath + "/tsdb-uid");
		config.overrideConfig("tsd.storage.hbase.tree_table", tablesBasePath + "/tsdb-tree");
		config.overrideConfig("tsd.storage.hbase.meta_table", tablesBasePath + "/tsdb-meta");
		config.overrideConfig("tsd.storage.flush_interval", String.valueOf(flushInterval));
		config.overrideConfig("tsd.storage.hbase.zk_quorum", quorum);
		// This isn't used by MapR because tables are part of the filesystem
		config.overrideConfig("tsd.storage.hbase.zk_basedir", "/hbase");
		config.disableCompactions();
	}

	public void run(int startingYear, int totalYears, int threads) {
		ExecutorService es = Executors.newFixedThreadPool(threads);
		BlockingQueue<Future<ImportResult>> bq = new LinkedBlockingQueue<Future<ImportResult>>(totalYears * metricNames.size());
		CompletionService<ImportResult> cs = new ExecutorCompletionService<ImportResult>(es, bq);

		int totalJobs = 0;
		for (String metricName : metricNames) {
			for (int i = 0; i < totalYears; i++) {
				cs.submit(new ImportJob(importMode, metricName, tags, startingYear + i));
				totalJobs++;
			}
		}

		int resultCount = 0;
		ImportMetrics metrics = new ImportMetrics(importMode, tsdbPool.get(), jobId);
		try {
			while (resultCount < totalJobs) {
				ImportResult result = cs.take().get();
				metrics.add(result);
				LOG.info("Job completed: {}", result);
				resultCount++;
			}
		}
		catch (InterruptedException ex) {
			LOG.error("The thread was interrupted while waiting for results", ex);
			Thread.currentThread().interrupt();
		}
		catch (ExecutionException ex) {
			LOG.error("An error occurred while waiting for results", ex);
			Thread.currentThread().interrupt();
		}
		finally {
			es.shutdownNow();
			try {
				int i = 0;
				for (TSDB tsdb : tsdbInstances) {
					LOG.debug("Flushing TSDB client: {}", i++);
					tsdb.flush().joinUninterruptibly();
				}
			}
			catch (Exception ex) {
				LOG.error("error flushing tsdb", ex);
			}
			finally {
				metrics.complete();
			}

			/**
			 * We shutdown after we flush so we can take a snapshot of the time to perform runtime
			 * calculations. If we didn't care about that then we would just shutdown.
			 */
			int i = 0;
			for (TSDB tsdb : tsdbInstances) {
				LOG.debug("Shuttting down TSDB client: {}", i++);
				tsdb.shutdown();
			}
		}
	}

	private class ImportJob implements Callable<ImportResult> {

		private final String metricName;
		private final int year;
		private final Map<String, String> tags;
		private final ImportMode importMode;

		public ImportJob(ImportMode importMode, final String metricName, final Map<String, String> tags, final int year) {
			this.importMode = importMode;
			this.metricName = metricName;
			this.tags = tags;
			this.year = year;
		}

		@Override
		public ImportResult call() throws Exception {
			final TSDB tsdb = tsdbPool.get();

			Random rand = new Random();
			int sampleSize = 3600;
			int maxDays = 365;
			int hoursPerDay = 24;
			Calendar cal = Calendar.getInstance();
			cal.set(year, 0, 1, 0, 0, 0);
			long startTime = cal.getTimeInMillis() / 1000;

			long begin = System.currentTimeMillis();

			final WritableDataPoints batch = importMode == ImportMode.batch ? tsdb.newBatch(metricName, tags) : null;

			long totalPuts = 0;
			LOG.debug("Importing year {} for {}", year, metricName);
			for (int day = 1; day <= maxDays; day++) {
				for (int hour = 0; hour < hoursPerDay; hour++) {
					SamplePoint[] samples = Sampler.generateArray(sampleSize, startTime, rand);
					if (batch != null) {
						for (SamplePoint sample : samples) {
							batch.addPoint(sample.getTime(), sample.getValue());
						}
						batch.persist();
						totalPuts++;
					}
					else {
						for (SamplePoint sample : samples) {
							tsdb.addPoint(metricName, sample.getTime(), sample.getValue(), tags);
							totalPuts++;
						}
					}
					startTime += sampleSize;
				}
			}

			long totalDatapoints = sampleSize * maxDays * hoursPerDay;
			return new ImportResult(metricName, totalPuts, totalDatapoints, year, begin, System.currentTimeMillis());
		}
	}
}
