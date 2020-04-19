package application;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppTester {

	private static final long serialVersionUID = 1L;
	private static Logger logger = LoggerFactory.getLogger(AppTester.class);
	private static String APPLICATION_CONFIG_FILE = "app-config.json";

	public static void main(String[] args) {
		
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

		try {
			Properties props = new Properties();
			props.load(new FileInputStream("customlog4j.properties"));
			PropertyConfigurator.configure(props);
		}

		catch (Exception ex) {
			
			logger.error("The application has encountered exception while loading custom log4j properties file named: 'customlog4j.properties'"
					+ " Details: " + ex.getMessage());
			System.exit(0);
		}

		try {

			FileExtensionCounterApp app = new FileExtensionCounterApp();

			try {
				app.loadApplicationConfig(APPLICATION_CONFIG_FILE);

			} catch (FileNotFoundException ex) {

				logger.error("Application configuration file could not loaded. Details:" + ex.getMessage());
				System.exit(0);
			}
			
			logger.info("The application has started...");

			app.initializeEnvironment(args);
			app.readInputFile();
			app.transformData();
			app.computeExtensionCount();
			app.printExtensionSummary();
			app.writeOutPutFile();
			
			//logger.info("The application has completed...");

		}

		catch (Exception ex) {

			logger.error("The application has encountered exception." + " Details: " + ex.getMessage());
		}

	}

}
