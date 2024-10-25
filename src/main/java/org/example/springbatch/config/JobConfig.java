package org.example.springbatch.config;

import jakarta.persistence.EntityManagerFactory;
import org.example.springbatch.batch.CustomerProcessor;
import org.example.springbatch.batch.JobListener;
import org.example.springbatch.entity.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class JobConfig {

    private final JobListener jobListener;
    private final CustomerProcessor customerProcessor;
    private final EntityManagerFactory entityManagerFactory;
    private final TaskExecutor taskExecutor;

    public JobConfig(
            JobListener jobListener,
            CustomerProcessor customerProcessor,
            EntityManagerFactory entityManagerFactory,
            @Qualifier("batchTaskExecutor") TaskExecutor taskExecutor) {
        this.jobListener = jobListener;
        this.customerProcessor = customerProcessor;
        this.entityManagerFactory = entityManagerFactory;
        this.taskExecutor = taskExecutor;
    }

    @Bean
    public Job comparisonJob(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        Flow sequentialFlow = new FlowBuilder<Flow>("sequentialFlow")
                .start(step1(jobRepository, transactionManager))
                .next(step2(jobRepository, transactionManager))
                .build();

        Flow flow1 = new FlowBuilder<Flow>("flow1")
                .start(step3(jobRepository, transactionManager))
                .build();

        Flow flow2 = new FlowBuilder<Flow>("flow2")
                .start(step4(jobRepository, transactionManager))
                .build();

        Flow parallelFlow = new FlowBuilder<Flow>("parallelFlow")
                .split(taskExecutor)
                .add(flow1, flow2)
                .build();

        return new JobBuilder("comparisonJob", jobRepository)
                .preventRestart()
                .listener(jobListener)
                .start(sequentialFlow)
                .next(parallelFlow)
                .end()
                .build();
    }

    @Bean
    public Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("sequentialStep1", jobRepository)
                .<Customer, Customer>chunk(1000, transactionManager)
                .reader(customerReader1())
                .processor(customerProcessor)
                .writer(customerWriter())
                .build();
    }

    @Bean
    public Step step2(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("sequentialStep2", jobRepository)
                .<Customer, Customer>chunk(1000, transactionManager)
                .reader(customerReader2())
                .processor(customerProcessor)
                .writer(customerWriter())
                .build();
    }

    @Bean
    public Step step3(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("parallelStep1", jobRepository)
                .<Customer, Customer>chunk(1000, transactionManager)
                .reader(customerReader3())
                .processor(customerProcessor)
                .writer(customerWriter())
                .build();
    }

    @Bean
    public Step step4(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("parallelStep2", jobRepository)
                .<Customer, Customer>chunk(1000, transactionManager)
                .reader(customerReader4())
                .processor(customerProcessor)
                .writer(customerWriter())
                .build();
    }

    // 각 Step에 별도의 Reader 사용
    @Bean
    @StepScope
    public JpaPagingItemReader<Customer> customerReader1() {
        return new JpaPagingItemReaderBuilder<Customer>()
                .name("customerReader1")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(1000)
                .queryString("SELECT c FROM Customer c")
                .build();
    }

    @Bean
    @StepScope
    public JpaPagingItemReader<Customer> customerReader2() {
        return new JpaPagingItemReaderBuilder<Customer>()
                .name("customerReader2")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(1000)
                .queryString("SELECT c FROM Customer c")
                .build();
    }

    @Bean
    @StepScope
    public JpaPagingItemReader<Customer> customerReader3() {
        return new JpaPagingItemReaderBuilder<Customer>()
                .name("customerReader3")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(1000)
                .queryString("SELECT c FROM Customer c")
                .build();
    }

    @Bean
    @StepScope
    public JpaPagingItemReader<Customer> customerReader4() {
        return new JpaPagingItemReaderBuilder<Customer>()
                .name("customerReader4")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(1000)
                .queryString("SELECT c FROM Customer c")
                .build();
    }

    @Bean
    @StepScope
    public JpaItemWriter<Customer> customerWriter() {
        return new JpaItemWriterBuilder<Customer>()
                .entityManagerFactory(entityManagerFactory)
                .build();
    }
}
