
package com.acheron.connector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acheron.connector.model.request.SpannerRequest;
import com.acheron.connector.model.response.SpannerResponse;
import com.acheron.connector.service.CreateTableImpl;
import com.acheron.connector.service.DeleteDataImpl;
import com.acheron.connector.service.InsertDataImpl;
import com.acheron.connector.service.RetrieveDataImpl;
import com.acheron.connector.service.UpdateDataImpl;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;

import io.camunda.connector.api.annotation.OutboundConnector;
import io.camunda.connector.api.outbound.OutboundConnectorContext;
import io.camunda.connector.api.outbound.OutboundConnectorFunction;

/**
 * @author AkashRamP
 *
 */
@OutboundConnector(name = "SPANNER", inputVariables = { "spannerConnection", "operation",
		"input" }, type = "com.acheron.camundaconnectors.db:spanner:1")
public class SpannerConnectorFunction implements OutboundConnectorFunction {

	private static final Logger LOGGER = LoggerFactory.getLogger(SpannerConnectorFunction.class);

	
	

	/**
	 * This method will call the executeRequest method to execute the request from the user.
	 */
	@Override
	public Object execute(OutboundConnectorContext context) throws Exception {

		final var request = context.bindVariables(SpannerRequest.class);

		

		SpannerResponse message = executeRequest(request);
		LOGGER.info("Your request is executed");
		return message;
	}

	/**
	 * @param request
	 * This method will execute the request by calling the respective class 
	 * based on the Operation Type given by the user.
	 * @return SpannerResponse
	 */
	public SpannerResponse executeRequest(SpannerRequest request) {

		String operationType = request.getOperation().getOperationType();
		SpannerOptions options = SpannerConnectorRuntimeApplication.getOptions();
		Spanner spanner = options.getService();
		DatabaseId databaseId = DatabaseId.of(request.getSpannerConnection().getProjectId(),
				request.getSpannerConnection().getInstanceName(), request.getSpannerConnection().getDatabaseName());
		DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();

		DatabaseClient databaseClient = spanner.getDatabaseClient(databaseId);

		if (operationType.equalsIgnoreCase("spanner.create-table")) {

			CreateTableImpl create = new CreateTableImpl();
			return create.createTable(dbAdminClient, request);
		}

		else if (operationType.equalsIgnoreCase("spanner.insert-data")) {
			InsertDataImpl insert = new InsertDataImpl();
			return insert.insertData(databaseClient, request);
		}
		
		else if (operationType.equalsIgnoreCase("spanner.update-data")) {
			UpdateDataImpl update = new UpdateDataImpl();
			return update.updateData(databaseClient, request);
		}
		
		else if (operationType.equalsIgnoreCase("spanner.delete-data")) {
			DeleteDataImpl delete = new DeleteDataImpl();
			return delete.deleteData(databaseClient, request);
		}
		else if (operationType.equalsIgnoreCase("spanner.retrieve-data")) {
			RetrieveDataImpl retrieve = new RetrieveDataImpl();
			return  retrieve.retrieveData(databaseClient, request);
		}

		return null;

	}

}
