package com.guptaji.threadPoolAndPartitionDemo.config;

import static com.guptaji.threadPoolAndPartitionDemo.constant.Constant.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@StepScope
@Component
public class CustomerPartitioner implements Partitioner, StepExecutionListener {

  Logger LOG = LogManager.getLogger(CustomerPartitioner.class);

  @Value("#{stepExecution}")
  public StepExecution stepExecution;

  //  private StepExecution stepExecution;

  @Override
  public Map<String, ExecutionContext> partition(int gridSize) {
    LOG.info("Executing partitioner");
    Map<String, ExecutionContext> executionContextMap = new HashMap<>(gridSize);
    ExecutionContext executionContext = this.stepExecution.getJobExecution().getExecutionContext();
    long count = executionContext.getLong(COUNT);
    LOG.info("Total count of records {}", count);

    // no. of records a grid can accomodate
    long recordsPerThread = (count / gridSize) + 1L;
    int size = gridSize;

    if (recordsPerThread < gridSize) {
      size = (int) recordsPerThread;
    }
    long start = 0L;
    for (int i = 0; i < size; i++) {
      ExecutionContext context = new ExecutionContext();
      context.putLong(PAGE_NO, start);
      context.putLong(NO_OF_ITEMS, recordsPerThread);
      context.putString(THREAD, THREAD + "_" + i);
      executionContextMap.put(PARTITION_NO + i, context);
      start = start + recordsPerThread;
    }
    return executionContextMap;
  }

  //  @Override
  //  public void beforeStep(StepExecution stepExecution) {
  //    this.stepExecution = stepExecution;
  //  }
  //
  //  @Override
  //  public ExitStatus afterStep(StepExecution stepExecution) {
  //    return null;
  //  }
}
