package com.acheron.connector.model.request;

import java.util.List;
import java.util.Map;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author AkashRamP
 *
 */
@Data
@NoArgsConstructor
public class Input {
	
	private String tableName;
	private List<Map<String,Object>> columnList;
	private String primaryKey;
	private String setColumnName;
	private String setValue;
	private String whereColumnName;
	private String whereValue;
	private String displayColumns;
	private String operatorType;
	private String displayType;
	private String orderColumnName;
	private String orderType;
	private String limit;
	

}
