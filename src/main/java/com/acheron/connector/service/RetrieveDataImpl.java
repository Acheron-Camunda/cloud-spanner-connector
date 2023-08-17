package com.acheron.connector.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acheron.connector.model.request.SpannerRequest;
import com.acheron.connector.model.response.QueryResponse;
import com.acheron.connector.model.response.SpannerResponse;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type;
import com.google.gson.Gson;

/**
 * @author AkashRamP
 *
 */
public class RetrieveDataImpl {

	private static final Logger LOGGER = LoggerFactory.getLogger(RetrieveDataImpl.class);

	/**
	 * @param databaseClient
	 * This method will form a query string based on the user's input and execute the executeQuery method
	 * @param request
	 * @return
	 */
	public SpannerResponse retrieveData(DatabaseClient databaseClient, SpannerRequest request) {

		LOGGER.info("inside retrieveData");
		String query = null;
		String select = "SELECT ";
		String from = " FROM ";

		QueryResponse<List<Map<String, Object>>> queryResponse = null;

		if (request.getInput().getDisplayType().equalsIgnoreCase("*")) {
			if (request.getInput().getWhereColumnName().equalsIgnoreCase("null")) {
				query = select + request.getInput().getDisplayType() + from + request.getInput().getTableName();
			} else {

				String whereValue = request.getInput().getWhereValue().replace("\\", "");

				query = select + request.getInput().getDisplayType() + from + request.getInput().getTableName()
						+ " WHERE " + request.getInput().getWhereColumnName() + " "
						+ request.getInput().getOperatorType() + " " + whereValue;
			}
		}

		else if (request.getInput().getDisplayType().equalsIgnoreCase("selectedColumns")) {
			if (request.getInput().getWhereColumnName().equalsIgnoreCase("null")) {
				query = select + request.getInput().getDisplayColumns() + from + request.getInput().getTableName();
			} else {
				String whereValue = request.getInput().getWhereValue().replace("\\", "");

				query = select + request.getInput().getDisplayColumns() + from + request.getInput().getTableName()
						+ " WHERE " + request.getInput().getWhereColumnName() + " "
						+ request.getInput().getOperatorType() + " " + whereValue;
			}

		}

		if (!(request.getInput().getOrderColumnName().equalsIgnoreCase("null"))) {
			query = query + " ORDER BY " + request.getInput().getOrderColumnName() + " "
					+ request.getInput().getOrderType();
		}

		if (!(request.getInput().getLimit().equalsIgnoreCase("null"))) {

			query = query + " LIMIT " + request.getInput().getLimit();

		}

		LOGGER.info(query);

		ResultSet resultSet = executeQuery(query, databaseClient);

		List<Map<String, Object>> resultList = new ArrayList<>();
		while (resultSet.next()) {
			Map<String, Object> rowMap = new HashMap<>();

			// Add each column value to the rowMap
			for (Type.StructField field : resultSet.getCurrentRowAsStruct().getType().getStructFields()) {
				String columnName = field.getName();

				Object columnValue;

				// Check the type of the field and get the appropriate value
				if (field.getType().getCode() == Type.Code.STRING) {
					columnValue = resultSet.getString(columnName);
				} else if (field.getType().getCode() == Type.Code.INT64) {
					columnValue = resultSet.getLong(columnName);
				} else {
					// Handle other data types if needed
					columnValue = null; // Or any appropriate default value
				}

				rowMap.put(columnName, columnValue);
			}

			// Add the rowMap to the resultList
			resultList.add(rowMap);
		}

		Gson gson = new Gson();
		String json = gson.toJson(resultList);

		LOGGER.info(json);

		List<Map<String, Object>> jsonList = gson.fromJson(json, List.class);

		queryResponse = new QueryResponse<>(jsonList);

		return queryResponse;

	}

	/**
	 * @param query
	 * This method will execute the query and retrieve data from the Spanner Database
	 * @param databaseClient
	 * @return
	 */
	private ResultSet executeQuery(String query, DatabaseClient databaseClient) {
		LOGGER.info("inside executeQuery");

		try (ReadOnlyTransaction transaction = databaseClient.readOnlyTransaction()) {
			return transaction.executeQuery(Statement.of(query));

		}

	}

}
