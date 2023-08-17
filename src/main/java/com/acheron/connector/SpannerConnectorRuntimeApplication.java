
package com.acheron.connector;

import java.io.FileInputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acheron.connector.exception.InvalidCredentialsException;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;


/**
 * @author AkashRamP
 *
 */
public class SpannerConnectorRuntimeApplication {

	Spanner spanner = null;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SpannerConnectorRuntimeApplication.class);

	/**
	 * This method is used to establish a connection to the Cloud Spanner Database.
	 * @return
	 */
	public static SpannerOptions getOptions() {
		SpannerOptions options = null;
		String jsonKeyPath = System.getenv("JSON_CREDENTIALS_FILEPATH");
		try {
			options = SpannerOptions.newBuilder()
					.setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(jsonKeyPath))).build();
			LOGGER.info("Connection Established");
		} catch (IOException e) {
			throw new InvalidCredentialsException("Invalid credentials in provided  Json File");
		}
		return options;
	}

}
