package com.guptaji.threadPoolAndPartitionDemo.repository;

import com.guptaji.threadPoolAndPartitionDemo.entity.Customer;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CustomerRepo extends JpaRepository<Customer, Integer> {}
