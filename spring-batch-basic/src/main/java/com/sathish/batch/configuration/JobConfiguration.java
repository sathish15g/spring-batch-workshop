package com.sathish.batch.configuration;

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.validator.ValidatingItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import com.sathish.batch.processor.FilteringItemProcessor;
import com.sathish.batch.processor.RetryItemProcessor;
import com.sathish.batch.processor.StudentRetryableException;
import com.sathish.batch.validator.StudentValidator;
import com.sathish.domain.Student;




@Configuration
public class JobConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

    @Bean
    public FlatFileItemReader<Student> fileReader() throws Exception {

        return new FlatFileItemReaderBuilder<Student>()
                .name("file-reader")
                .resource(new ClassPathResource("students.csv"))
                .targetType(Student.class)
                .linesToSkip(1)
                .delimited().delimiter(",").names(new String[]{"firstName", "lastName",  "email", "age"})
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<Student> jdbcWriter() {
        return new JdbcBatchItemWriterBuilder<Student>()
                .dataSource(dataSource)
                .sql("insert into STUDENT( FIRST_NAME, LAST_NAME, EMAIL, AGE) values (:firstName, :lastName, :email, :age)")
                .beanMapped()
                .build();
    }
    
	@Bean
	public CompositeItemProcessor<Student, Student> itemProcessor(DataSource dataSource) throws Exception {

		List<ItemProcessor<Student, Student>> delegates = new ArrayList<>(2);

		delegates.add(ValidteItemProcessor());
		delegates.add(retryProcessor());
		delegates.add(new FilteringItemProcessor(dataSource));
		

		CompositeItemProcessor<Student, Student> compositeItemProcessor =
				new CompositeItemProcessor<>();

		compositeItemProcessor.setDelegates(delegates);
		compositeItemProcessor.afterPropertiesSet();

		return compositeItemProcessor;
	}
	

	@Bean
	public ValidatingItemProcessor<Student> ValidteItemProcessor() {
		ValidatingItemProcessor<Student> customerValidatingItemProcessor =
				new ValidatingItemProcessor<>(new StudentValidator());

		customerValidatingItemProcessor.setFilter(true);

		return customerValidatingItemProcessor;
	}

	@Bean
	@StepScope
	public RetryItemProcessor retryProcessor() {
		RetryItemProcessor processor = new RetryItemProcessor();
		return processor;
	}
	
	@Bean
	public Step step1() throws Exception {
		return stepBuilderFactory.get("step1")
				.<Student, Student>chunk(10)
				.reader(fileReader())
				.processor(itemProcessor(dataSource))
				.writer(jdbcWriter())
				.faultTolerant()
				.retry(StudentRetryableException.class)
				.retryLimit(5)
				.build();
	}

	@Bean
	public Job job() throws Exception {	
		return jobBuilderFactory.get("job")
				.incrementer(new RunIdIncrementer())
				.start(step1())
				.build();
	}
	
}
