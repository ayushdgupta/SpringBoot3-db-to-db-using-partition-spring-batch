package com.guptaji.threadPoolAndPartitionDemo.config;

import com.guptaji.threadPoolAndPartitionDemo.entity.Customer;
import com.guptaji.threadPoolAndPartitionDemo.repository.CustomerRepo;
import com.guptaji.threadPoolAndPartitionDemo.service.CustomerProcessor;
import com.guptaji.threadPoolAndPartitionDemo.tasklet.RecordCounterTasklet;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemReader;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class SpringBatchConfig {

  @Autowired private DataSource dataSource;

  @Autowired private CustomerRepo customerRepo;

  @Autowired private CustomerPartitioner customerPartitioner;

  @Value("${GRID_SIZE}")
  public int gridSize;

  @Value("${CHUNK_SIZE}")
  public int chunkSize;

  // tasklet to read count
  @Bean
  public RecordCounterTasklet recordCounterTasklet() {
    return new RecordCounterTasklet();
  }

  // item reader
  @Bean
  public RepositoryItemReader<Customer> customerItemReader() {
    RepositoryItemReader<Customer> reader = new RepositoryItemReader<>();
    reader.setRepository(customerRepo);
    reader.setMethodName("findAll");
    reader.setPageSize(chunkSize * gridSize);
    Map<String, Direction> sorts = new HashMap<>();
    sorts.put("id", Direction.ASC);
    reader.setSort(sorts);
    reader.setSaveState(false);

    return reader;

    //        Map<String, Direction> sortMap = new HashMap<>();
    //        sortMap.put("id", Direction.DESC);
    //
    //        return new RepositoryItemReaderBuilder<MyItem>()
    //                .repository(repository)
    //                .methodName("findTempByStatus")
    //                .arguments(Arrays.asList(1L, 2L))
    //                .sorts(sortMap)
    //                .saveState(false)
    //                .build();
  }

  @Bean
  public CustomerProcessor customerProcessor() {
    return new CustomerProcessor();
  }

  @Bean
  public RepositoryItemWriter<Customer> customWriter() {
    RepositoryItemWriter<Customer> writer = new RepositoryItemWriter<>();
    writer.setRepository(customerRepo);
    writer.setMethodName("saveAll");
    return writer;
  }

  // task Executor
  @Bean
  public TaskExecutor taskExecutor() {
    ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
    taskExecutor.setMaxPoolSize(gridSize);
    taskExecutor.setCorePoolSize(gridSize);
    taskExecutor.setQueueCapacity(gridSize);
    return taskExecutor;
  }

  // Partitioner that we will use
  //  @Bean
  //  public CustomerPartitioner customerPartitioner() {
  //    return new CustomerPartitioner();
  //  }

  // partition Handler
  @Bean
  public PartitionHandler customerPartitionHandler(@Qualifier("slaveStep") Step slaveStep) {
    TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
    handler.setGridSize(gridSize);
    handler.setStep(slaveStep);
    handler.setTaskExecutor(taskExecutor());
    return handler;
  }

  // master - slave step
  @Bean
  public Step slaveStep(
      JobRepository jobRepository, PlatformTransactionManager transactionManager) {
    return new StepBuilder("slaveStep", jobRepository)
        .<Customer, Customer>chunk(chunkSize, transactionManager)
        .reader(customerItemReader())
        .processor(customerProcessor())
        .writer(customWriter())
        .build();
  }

  @Bean
  public Step masterStep(
      JobRepository jobRepository, PlatformTransactionManager transactionManager) {
    return new StepBuilder("masterStep", jobRepository)
        .partitioner(slaveStep(jobRepository, transactionManager).getName(), customerPartitioner)
        .partitionHandler(customerPartitionHandler(slaveStep(jobRepository, transactionManager)))
        .build();
  }

  @Bean
  public Step taskletStep(
      JobRepository jobRepository, PlatformTransactionManager transactionManager) {
    return new StepBuilder("taskletStep", jobRepository)
        .tasklet(recordCounterTasklet(), transactionManager)
        .build();
  }

  @Bean
  public Job customerJob(
      JobRepository jobRepository, PlatformTransactionManager transactionManager) {
    return new JobBuilder("Customer_JOB_With_Partitioner", jobRepository)
        .start(taskletStep(jobRepository, transactionManager))
        .next(masterStep(jobRepository, transactionManager))
        .build();
  }
}
