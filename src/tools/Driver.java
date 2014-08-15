package net.opentsdb.tools;

import net.opentsdb.tools.load.BulkImport;

/**
 *
 * @author jscott
 */
public class Driver {

	public static void main(String[] args) throws Exception {
		boolean validRequest = false;
		String action = null;
		for (int i = 0; i < args.length && !validRequest; i++) {
			if (args[i].startsWith("--action=")) {
				action = args[i].split("=")[1];
				validRequest = true;
			}
		}
		if (validRequest) {
			if ("BULK".equalsIgnoreCase(action)) {
				BulkImport.main(args);
			}
			else if ("TEXT".equalsIgnoreCase(action)) {
				TextImporter.main(args);
			}
			else {
				validRequest = false;
			}
		}

		if (!validRequest) {
			System.err.println("No valid '--action=?' was specified. '" + action + "'");
			System.err.println("Valid actions are: bulk, text");
		}
	}
}
