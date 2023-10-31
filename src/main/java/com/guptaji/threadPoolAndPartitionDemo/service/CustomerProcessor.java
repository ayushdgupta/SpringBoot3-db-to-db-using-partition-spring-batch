package com.guptaji.threadPoolAndPartitionDemo.service;

import com.guptaji.threadPoolAndPartitionDemo.constant.Constant;
import com.guptaji.threadPoolAndPartitionDemo.entity.Customer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.item.ItemProcessor;

public class CustomerProcessor implements ItemProcessor<Customer, Customer> {

  Logger LOG = LogManager.getLogger(CustomerProcessor.class);

  @Override
  public Customer process(Customer item) throws Exception {
    //    LOG.info("process the record {}", item.getId());
    item.setCountry(Constant.COUNTRY);
    return null;
  }
}
