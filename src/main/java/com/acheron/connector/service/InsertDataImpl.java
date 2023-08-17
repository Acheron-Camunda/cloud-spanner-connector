package com.acheron.connector.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
public class InsertDataImpl {

	private static final Logger LOGGER = LoggerFactory.getLogger(InsertDataImpl.class);

	QueryResponse<String> queryResponse;

	/**
	 * @param databaseClient This method will call the executeQuery method to
	 *                       execute the query
	 * @param request
	 * @return
	 */
	public SpannerResponse insertData(DatabaseClient databaseClient, SpannerRequest request) {
		LOGGER.info("Inside insertData");

		List<String> colNames = new ArrayList<>();
		List<Object> values = new ArrayList<>();

//		 insertDataParameterizedQuery(request.getInput().getTableName(),request.getInput().getColumnList() , null)

		List<Map<String, Object>> data = request.getInput().getColumnList();

		for (Map<String, Object> map : data) {

			for (Map.Entry<String, Object> entry : map.entrySet()) {
				String key = entry.getKey();
				Object value = entry.getValue();
				colNames.add(key);
				values.add(value);
			}
		}

		colNames = colNames.stream().distinct().collect(Collectors.toList());

		revCols(colNames);
		revValues(values);

		String finalQuery = insertDataParameterizedQuery(request.getInput().getTableName(), colNames, values);

		LOGGER.info("Inserting the data to {}", request.getInput().getTableName());
		return executeQuery(finalQuery, databaseClient);

	}

	/**
	 * @param tableName
	 * @param colNames
	 * @param values    This method provides the query to be executed to insert data
	 *                  to the Spanner Database.
	 * @return
	 */
	private String insertDataParameterizedQuery(String tableName, List<String> colNames, List<Object> values) {

		String columnNames = colNames.stream().map(String::valueOf).collect(Collectors.joining(", "));
		String valuesList = values.stream().map(String::valueOf).collect(Collectors.joining(", "));
		String crtValues = valuesList.replace("\\", "");
		LOGGER.info(crtValues);
		List<Integer> indexes = new ArrayList<>();

		for (int i = 0; i < crtValues.length(); i++) {
			if (crtValues.charAt(i) == ',') {
				indexes.add(i);
			}
		}

		String finalValues = crtValues;
		if (indexes.size() > 2) {

			int indexToReplace = indexes.get(2);

			String part1 = crtValues.substring(0, indexToReplace);
			String part2 = crtValues.substring(indexToReplace + 1);
			finalValues = part1 + "), (" + part2;
		}

		StringBuilder insertQuery = new StringBuilder(
				"INSERT INTO " + tableName + "(" + columnNames + ") VALUES (" + finalValues + ")");

		return insertQuery.toString();

	}

	/**
	 * @param insertQuery
	 * @param dbClient    This method will execute the query and inserts the data to
	 *                    the Spanner Database.
	 * @return
	 */
	private SpannerResponse executeQuery(String insertQuery, DatabaseClient dbClient) {

		if (dbClient.readWriteTransaction().run(transaction -> {
			transaction.executeUpdate(Statement.of(insertQuery));

			return null;
		}) == null) {

			queryResponse = new QueryResponse<>("Your data is inserted");
			return queryResponse;
		} else {
			queryResponse = new QueryResponse<>("Data is not inserted");
			return queryResponse;
		}

	}

	/**
	 * @param colNames This method reverse the List<String> colNames.
	 * @return
	 */
	private List<String> revCols(List<String> colNames) {
		if (colNames.size() <= 1)
			return Collections.emptyList();

		String value = colNames.remove(0);

		revCols(colNames);

		colNames.add(value);
		return colNames;
	}

	/**
	 * @param values This method reverse the List<Object> values.
	 * @return
	 */
	private List<Object> revValues(List<Object> values) {
		if (values.size() <= 1)
			return Collections.emptyList();

		Object value = values.remove(0);

		revValues(values);

		values.add(value);
		return values;
	}

}
