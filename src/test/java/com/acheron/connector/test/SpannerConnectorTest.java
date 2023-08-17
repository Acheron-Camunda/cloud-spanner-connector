package com.acheron.connector.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import com.acheron.connector.model.request.Input;
import com.acheron.connector.model.request.Operation;
import com.acheron.connector.model.request.SpannerConnection;
import com.acheron.connector.model.request.SpannerRequest;
import com.acheron.connector.model.response.QueryResponse;
import com.acheron.connector.model.response.SpannerResponse;
import com.acheron.connector.service.CreateTableImpl;
import com.acheron.connector.service.DeleteDataImpl;
import com.acheron.connector.service.InsertDataImpl;
import com.acheron.connector.service.UpdateDataImpl;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;

import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

@ExtendWith(SystemStubsExtension.class)
class SpannerConnectorTest {

	private static SpannerOptions options;
	private static CreateTableImpl createTable;
	private static CreateTableImpl mockCreate;
	private static Spanner spanner;
	private static UpdateDataImpl updateData;
	private static UpdateDataImpl mockUpdate;
	private static InsertDataImpl insertData;
	private static InsertDataImpl mockInsert;
	private static DeleteDataImpl deleteData;
	private static DeleteDataImpl mockDelete;
	

	@BeforeAll
	public static void setup() {
		String jsonKeyPath = "C:\\Spanner\\spanner-key.json";

		try {
			options = SpannerOptions.newBuilder()
					.setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(jsonKeyPath))).build();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		mockCreate = Mockito.mock(CreateTableImpl.class);
		createTable = new CreateTableImpl();

		mockUpdate = Mockito.mock(UpdateDataImpl.class);
		updateData = new UpdateDataImpl();
		
		mockInsert = Mockito.mock(InsertDataImpl.class);
		insertData = new InsertDataImpl();
		
		mockDelete = Mockito.mock(DeleteDataImpl.class);
		deleteData = new DeleteDataImpl();

	}

	@AfterAll
	public static void teardown() {
		options = null;
		mockCreate = null;
	}

	@Test
	void testCreateTable() {
		spanner = options.getService();
		DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();
		SpannerRequest spannerRequest = getCreateSpannerRequest();

		QueryResponse<String> expectedSpannerResponse = new QueryResponse<String>();

		expectedSpannerResponse.setResponse(spannerRequest.getInput().getTableName() + " is created in Cloud Spanner");

		when(mockCreate.createTable(dbAdminClient, spannerRequest)).thenReturn(expectedSpannerResponse);

		SpannerResponse actualResponse = createTable.createTable(dbAdminClient, spannerRequest);

		assertEquals(expectedSpannerResponse, actualResponse);

	}
	
	@Test
	void testInsertData() {
		spanner = options.getService();
		DatabaseId databaseId = DatabaseId.of(getSpannerConnection().getProjectId(),
				getSpannerConnection().getInstanceName(), getSpannerConnection().getDatabaseName());
		DatabaseClient databaseClient = spanner.getDatabaseClient(databaseId);
		
		SpannerRequest spannerRequest = getInsertSpannerRequest();
		
		QueryResponse<String> expectedSpannerResponse = new QueryResponse<String>();
		
		expectedSpannerResponse.setResponse("Your data is inserted");
		
		when(mockInsert.insertData(databaseClient, spannerRequest)).thenReturn(expectedSpannerResponse);
		
		SpannerResponse actualResponse = insertData.insertData(databaseClient, spannerRequest);
		
		assertEquals(expectedSpannerResponse, actualResponse);
	}
	
	
	
	

	@Test
	void testUpdateData() {
		spanner = options.getService();
		DatabaseId databaseId = DatabaseId.of(getSpannerConnection().getProjectId(),
				getSpannerConnection().getInstanceName(), getSpannerConnection().getDatabaseName());
		DatabaseClient databaseClient = spanner.getDatabaseClient(databaseId);

		SpannerRequest spannerRequest = getUpdateSpannerRequest();

		QueryResponse<String> expectedSpannerResponse = new QueryResponse<String>();

		expectedSpannerResponse.setResponse("Your data is updated");

		when(mockUpdate.updateData(databaseClient, spannerRequest)).thenReturn(expectedSpannerResponse);

		SpannerResponse actualResponse = updateData.updateData(databaseClient, spannerRequest);

		assertEquals(expectedSpannerResponse, actualResponse);

	}
	
	@Test
	void testDeleteData() {
		spanner = options.getService();
		DatabaseId databaseId = DatabaseId.of(getSpannerConnection().getProjectId(),
				getSpannerConnection().getInstanceName(), getSpannerConnection().getDatabaseName());
		DatabaseClient databaseClient = spanner.getDatabaseClient(databaseId);
		
		SpannerRequest spannerRequest = getDeleteSpannerRequest();
		
		QueryResponse<String> expectedSpannerResponse = new QueryResponse<String>();
		
		expectedSpannerResponse.setResponse("Your data is deleted");
		
		when(mockDelete.deleteData(databaseClient, spannerRequest)).thenReturn(expectedSpannerResponse);
		SpannerResponse actualResponse = deleteData.deleteData(databaseClient, spannerRequest);
		
		assertEquals(expectedSpannerResponse, actualResponse);
		
		
	}


	SpannerRequest getCreateSpannerRequest() {
		SpannerConnection spannerConnection = getSpannerConnection();
		Operation operation = getOperationCreate();
		Input input = getCreateInput();
		SpannerRequest spannerRequest = new SpannerRequest();
		spannerRequest.setSpannerConnection(spannerConnection);
		spannerRequest.setOperation(operation);
		spannerRequest.setInput(input);
		return spannerRequest;
	}

	SpannerRequest getUpdateSpannerRequest() {
		SpannerConnection spannerConnection = getSpannerConnection();
		Operation operation = getOperationUpdate();
		Input input = getUpdateInput();
		SpannerRequest spannerRequest = new SpannerRequest();
		spannerRequest.setSpannerConnection(spannerConnection);
		spannerRequest.setOperation(operation);
		spannerRequest.setInput(input);
		return spannerRequest;
	}

	SpannerRequest getInsertSpannerRequest() {
		SpannerConnection spannerConnection = getSpannerConnection();
		Operation operation = getOperationInsert();
		Input input = getInsertInput();
		SpannerRequest spannerRequest = new SpannerRequest();
		spannerRequest.setSpannerConnection(spannerConnection);
		spannerRequest.setOperation(operation);
		spannerRequest.setInput(input);
		return spannerRequest;
	}
	
	SpannerRequest getDeleteSpannerRequest() {
		SpannerConnection spannerConnection = getSpannerConnection();
		Operation operation = getOperationDelete();
		Input input = getDeleteInput();
		SpannerRequest spannerRequest = new SpannerRequest();
		spannerRequest.setSpannerConnection(spannerConnection);
		spannerRequest.setOperation(operation);
		spannerRequest.setInput(input);
		return spannerRequest;
	}
	
	
	
	

//	spannerConnection
	SpannerConnection getSpannerConnection() {

		SpannerConnection spannerConnection = new SpannerConnection();
		spannerConnection.setProjectId("camunda-dev-2024");
		spannerConnection.setInstanceName("camunda-connector");
		spannerConnection.setDatabaseName("connector");

		return spannerConnection;

	}

//	 Create
	Operation getOperationCreate() {
		Operation operation = new Operation();
		operation.setOperationType("spanner.create-table");
		return operation;
	}

	Input getCreateInput() {
		Input input = new Input();
		input.setTableName("Library");
		input.setPrimaryKey("bookId");
		input.setColumnList(getColumns());
		return input;
	}

	List<Map<String, Object>> getColumns() {

		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("colName", "bookId");
		map.put("dataType", "INT64");
		list.add(map);

		return list;

	}
	
	
	
//	insert
	Operation getOperationInsert() {
		Operation operation = new Operation();
		operation.setOperationType("spanner.insert-data");
		return operation;
	}
	
	Input getInsertInput() {
		Input input = new Input();
		input.setTableName("Library");
		input.setColumnList(getColumnsList());
		return input;
	}
	
	List<Map<String,Object>> getColumnsList(){
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("bookId", 143);
		map.put("bookName", "\"Two States\"");
		list.add(map);
		return list;
	}

//	 Update
	Operation getOperationUpdate() {
		Operation operation = new Operation();
		operation.setOperationType("spanner.update-data");
		return operation;
	}

	Input getUpdateInput() {
		Input input = new Input();
		input.setTableName("Library");
		input.setSetColumnName("bookName");
		input.setSetValue("\"Harry Potter\"");
		input.setWhereColumnName("bookId");
		input.setWhereValue("1");

		return input;
	}
	
//	Delete
	Operation getOperationDelete() {
		Operation operation = new Operation();
		operation.setOperationType("spanner.delete-data");
		return operation;
	}
	
	Input getDeleteInput() {
		Input input = new Input();
		input.setTableName("Library");
		input.setWhereColumnName("bookId");
		input.setWhereValue("2");
		return input;
	}
	

}
