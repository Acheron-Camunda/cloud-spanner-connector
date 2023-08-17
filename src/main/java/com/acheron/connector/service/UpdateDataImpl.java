package com.acheron.connector.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acheron.connector.model.request.SpannerRequest;
import com.acheron.connector.model.response.QueryResponse;
import com.acheron.connector.model.response.SpannerResponse;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Statement;

/**
 * @author AkashRamP
 *
 */
public class UpdateDataImpl {

	private static final Logger LOGGER = LoggerFactory.getLogger(UpdateDataImpl.class);

	QueryResponse<String> queryResponse;

	/**
	 * @param databaseClient 
	 * This method will call the executeQuery method to
	 * execute the query.
	 * @param request
	 * @return
	 */
	public SpannerResponse updateData(DatabaseClient databaseClient, SpannerRequest request) {

		LOGGER.info("inside updateData method");

		String setValue = request.getInput().getSetValue().replace("\\", "");
		LOGGER.info(setValue);

		String query = "UPDATE " + request.getInput().getTableName() + " SET " + request.getInput().getSetColumnName()
				+ " = " + setValue + " WHERE " + request.getInput().getWhereColumnName() + " = "
				+ request.getInput().getWhereValue();

		return executeQuery(query, databaseClient);

	}

	/**
	 * @param query
	 * @param databaseClient 
	 * This method will execute the query and updates the data
	 * in the table.
	 * @return
	 */
	private SpannerResponse executeQuery(String query, DatabaseClient databaseClient) {

		if (databaseClient.readWriteTransaction().run(transaction -> {
			transaction.executeUpdate(Statement.of(query));

			return null;
		}) == null) {

			queryResponse = new QueryResponse<>("Your data is updated");
			return queryResponse;
		} else {
			queryResponse = new QueryResponse<>("Data is not updated");
			return queryResponse;
		}

	}

}
