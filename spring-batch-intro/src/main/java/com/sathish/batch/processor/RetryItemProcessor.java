package com.sathish.batch.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import com.sathish.domain.Student;


public class RetryItemProcessor implements ItemProcessor<Student, Student> {

	
	private static final Logger log = LoggerFactory.getLogger(RetryItemProcessor.class);

	private int attemptCount = 0;

	@Override
	public Student process(Student student) throws Exception {
		if(student.getAge()>30) {
			attemptCount++;

			if(attemptCount >= 3) {
				log.info("SUCCESS IN RETRY...");
				student.setAge(30);
				return student;
			}
			else {
				log.info("Processing of student " + student.getFirstName()+ " failed");
				throw new StudentRetryableException("Student age cannot be greater than 30.  Attempt:" + attemptCount);
			}
		}
		else {
			return student;
		}
	}

}
