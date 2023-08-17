package com.acheron.connector.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acheron.connector.model.request.SpannerRequest;
import com.acheron.connector.model.response.QueryResponse;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;

/**
 * @author AkashRamP
 *
 */
public class CreateTableImpl {

	private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableImpl.class);

	QueryResponse<String> queryResponse;

	/**
	 * @param databaseAdminClient
	 * @param request
	 * This method executes the query to create a table in the Cloud Spanner database.
	 * @return
	 */
	public QueryResponse<String> createTable(DatabaseAdminClient databaseAdminClient, SpannerRequest request) {
		LOGGER.info("inside create table method");

		String columns = getColumns(request.getInput().getColumnList());
		LOGGER.info(columns);

		Iterable<String> query = new ArrayList<>(Arrays.asList("CREATE TABLE " + request.getInput().getTableName()
				+ "(" + columns + ") PRIMARY KEY(" + request.getInput().getPrimaryKey() + ")"));

		LOGGER.info("Creating Table");
		OperationFuture<Void, UpdateDatabaseDdlMetadata> op = databaseAdminClient.updateDatabaseDdl(
				request.getSpannerConnection().getInstanceName(), request.getSpannerConnection().getDatabaseName(),
				query, null);

		if (!op.isDone()) {
			
			queryResponse = new QueryResponse<>(request.getInput().getTableName() + " is created in Cloud Spanner");
			return queryResponse;
		} else {
			queryResponse = new QueryResponse<>("Table is not Created");
			return queryResponse;
		}

	}

	/**
	 * @param columnList
	 * This method will convert List<Map<String,Object>> to String.
	 * @return
	 */
	private String getColumns(List<Map<String, Object>> columnList) {

		StringBuilder columns = new StringBuilder();
		boolean first = true;
		for (Map<String, Object> colMap : columnList) {
			if (colMap != null && !colMap.isEmpty()) {
				Map<String, Object> columnDetails = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
				columnDetails.putAll(colMap);
				String columnNameStr = columnDetails.getOrDefault("colName", "").toString();
				String dataTypeStr = columnDetails.getOrDefault("dataType", "").toString();
				if (first) {
					columns.append(columnNameStr).append(" ").append(dataTypeStr);
					first = false;
				} else {
					columns.append(",").append(columnNameStr).append(" ").append(dataTypeStr);
				}

			}
		}
		return columns.toString();
	}

}
