package com.sathish.batch.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StudentRetryableException extends Exception {

private static final Logger log = LoggerFactory.getLogger(StudentRetryableException.class);

	public StudentRetryableException() {
		super();
	}

	public StudentRetryableException(String msg) {
		super(msg);
		log.error(msg);
	}
}
