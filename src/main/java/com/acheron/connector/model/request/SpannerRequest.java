
//////////////////////////////////



package com.acheron.connector.model.request;

import lombok.Data;
import lombok.NoArgsConstructor;
@Data
@NoArgsConstructor
public class SpannerRequest {
	
	 private SpannerConnection spannerConnection;
	 private Operation operation;
	 private Input input;
	

	
	  

}
