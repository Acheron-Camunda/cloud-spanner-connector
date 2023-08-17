package com.acheron.connector.model.response;

import java.util.Objects;




/**
 * @author AkashRamP
 *
 * @param <T>
 */
public class QueryResponse<T> implements SpannerResponse {
	
	private T response;
	
	public QueryResponse(T response) {
	    this.response = response;
	  }

	public T getResponse() {
	    return response;
	  }
	
	
	

	public void setResponse(T response) {
		this.response = response;
	}

	public QueryResponse() {
		this.response = null;
	
	}

	

	@Override
	public int hashCode() {
		return Objects.hash(response);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		QueryResponse other = (QueryResponse) obj;
		return Objects.equals(response, other.response);
	}

	@Override
	public String toString() {
		return "QueryResponse [response=" + response + "]";
	}

	
	
}
