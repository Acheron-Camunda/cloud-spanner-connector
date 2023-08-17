package com.acheron.connector.model.request;

import lombok.Data;
@Data
public class SpannerConnection {
	
	private String projectId;
	private String instanceName;
	private String databaseName;
	

}
