package com.sathish.batch.processor;

import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import com.sathish.domain.Student;

public class FilteringItemProcessor implements ItemProcessor<Student, Student> {

	
	private static final Logger log = LoggerFactory.getLogger(FilteringItemProcessor.class);

	private DataSource dataSource;
	
	public FilteringItemProcessor(DataSource dataSource) {
		super();
		this.dataSource = dataSource;
	}


	@Override
	public Student process(Student student) throws Exception {

		String sql = "SELECT * FROM STUDENT  WHERE email = ?";
		 
		List<Student> studentRec = getJdbcTemplate().query(
				sql, new Object[] { student.getEmail() }, 
				new BeanPropertyRowMapper<>(Student.class));
		if(!studentRec.isEmpty()) {
			log.info("Student record already present skipping the record");
			return null;
		}else {
			//log.info("Student record not present persisting the record");
			return student;
		}		
	}

	private JdbcTemplate getJdbcTemplate() {
		return new JdbcTemplate(this.dataSource);
	}


}
