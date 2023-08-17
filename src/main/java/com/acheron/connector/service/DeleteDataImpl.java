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
public class DeleteDataImpl {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DeleteDataImpl.class);
	QueryResponse<String> queryResponse;
	
	
	/**
	 * @param databaseClient
	 * @param request
	 * This method will call the executeQuery method to execute the query
	 * @return
	 */
	public SpannerResponse deleteData(DatabaseClient databaseClient, SpannerRequest request) {
		
		LOGGER.info("inside deleteData");
		String value = request.getInput().getWhereValue().replace("\\", "");
		
		String query = "DELETE FROM "+request.getInput().getTableName()+" WHERE "+request.getInput().getWhereColumnName()+" = "+value;
		LOGGER.info(query);
		return executeQuery(query,databaseClient);
		
	}

	/**
	 * @param query
	 * @param databaseClient
	 * This method will execute the query and delete the data to the Spanner Database.
	 * @return
	 */
	private SpannerResponse executeQuery(String query, DatabaseClient databaseClient) {
		if(databaseClient.readWriteTransaction().run(transaction -> {
			transaction.executeUpdate(Statement.of(query));
			
			return null;
		}) == null) {
			
			queryResponse = new QueryResponse<>( "Your data is deleted");
			return queryResponse;
		} else {
			queryResponse = new QueryResponse<>( "Data is not deleted");
			return queryResponse;
		}
	}

}
