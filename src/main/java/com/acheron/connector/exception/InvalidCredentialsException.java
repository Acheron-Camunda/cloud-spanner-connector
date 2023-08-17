package com.acheron.connector.exception;

/**
 * @author AkashRamP
 * This exception will be thrown while read invalid Google credentials from stream
 *
 */
public class InvalidCredentialsException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public InvalidCredentialsException() {
		super();
	}

	public InvalidCredentialsException(String message) {
		super(message);
	}
	
	

}
