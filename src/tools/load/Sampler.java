package net.opentsdb.tools.load;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.Random;
import java.util.TimeZone;
import java.util.zip.GZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jscott
 */
public class Sampler {

	private static final Logger LOG = LoggerFactory.getLogger(Sampler.class);
	private static final String GENERIC_TAG = "x=y";
	private static final int RANDOM_RANGE = 101;
	private static final int RANDOM_GAP = 50;

	public static SamplePoint[] generateArray(final int sampleSize, final long startTime, final Random rand) {
		long time = startTime;
		long value = rand.nextInt(RANDOM_RANGE) - RANDOM_GAP;
		SamplePoint[] samples = new SamplePoint[sampleSize];
		for (int i = 0; i < sampleSize; i++) {
			// Alter the value by a range of +/- RANDOM_GAP
			value += rand.nextInt(RANDOM_RANGE) - RANDOM_GAP;
			samples[i] = new SamplePoint(time++, value);
		}
		return samples;
	}

	/**
	 *
	 * @param useCompression if true a .gz will be added to the end of each file name
	 * @param useMillis true to use milliseconds, false for seconds
	 * @param basePath /dfs/sensors/...
	 * @param metricName /dfs/sensors/year/week#/NAME.tsd(.gz)
	 * @param startYear 2003, etc... /dfs/sensors/YEAR/week#/name.tsd(.gz)
	 * @param totalYears /dfs/sensors/YEAR/week#/name.tsd(.gz)
	 * @param rand
	 */
	public static void generateWeeklyFiles(boolean useCompression, boolean useMillis, File basePath, String metricName, final int startYear, final int totalYears, final Random rand) throws IOException {
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		cal.set(startYear, 0, 1, 0, 0, 0);

		String extension = useCompression ? ".tsd.gz" : ".tsd";
		int sampleSize = useMillis ? 3600000 : 3600;
		long value = rand.nextInt(RANDOM_RANGE) - RANDOM_GAP;

		for (int year = startYear; year < startYear + totalYears; year++) {
			long time = useMillis ? cal.getTimeInMillis() : cal.getTimeInMillis() / 1000;
			long startTime = System.currentTimeMillis();
			for (int week = 1; week <= 52; week++) {
				File metricFolder = new File(basePath, year + "/" + (week < 10 ? "0" + week : week));
				metricFolder.mkdirs();
				File metricFile = new File(metricFolder, metricName + extension);
				OutputStream os = createOutputStream(useCompression, metricFile);
				for (int day = 0; day < 7; day++) {
					for (int hour = 0; hour < 24; hour++) {
						for (int i = 0; i < sampleSize; i++) {
							writeRecord(os, metricName, time, value);
							// Alter the value by a range of +/- RANDOM_GAP
							value += rand.nextInt(RANDOM_RANGE) - RANDOM_GAP;
							time++;
						}
					}
				}
				os.flush();
				os.close();
			}
			cal.add(Calendar.YEAR, 1);
			long totalTime = System.currentTimeMillis() - startTime;
			LOG.info("Total time to create {}-{} weekly time series data: {}ms", metricName, year, totalTime);
		}
	}

	private static OutputStream createOutputStream(boolean useCompression, File path) throws IOException {
		FileOutputStream fos = new FileOutputStream(path);
		return useCompression
				? new BufferedOutputStream(new GZIPOutputStream(fos))
				: new BufferedOutputStream(fos);
	}

	private static void writeRecord(OutputStream os, String metricName, long time, long value) throws IOException {
		String record = metricName + " " + time + " " + value + " " + GENERIC_TAG + "\n";
		os.write(record.getBytes());
	}

	public static class SamplePoint {

		private final long time;
		private final long value;

		public SamplePoint(final long time, final long value) {
			this.time = time;
			this.value = value;
		}

		public long getTime() {
			return time;
		}

		public long getValue() {
			return value;
		}
	}
}
