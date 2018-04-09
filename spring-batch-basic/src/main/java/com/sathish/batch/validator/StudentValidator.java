package com.sathish.batch.validator;

import org.springframework.batch.item.validator.ValidationException;
import org.springframework.batch.item.validator.Validator;

import com.sathish.domain.Student;

public class StudentValidator implements Validator<Student> {

	@Override
	public void validate(Student value) throws ValidationException {
		if(value.getFirstName().startsWith("A")) {
			throw new ValidationException("First names that begin with A are invalid: " + value);
		}
	}
}
