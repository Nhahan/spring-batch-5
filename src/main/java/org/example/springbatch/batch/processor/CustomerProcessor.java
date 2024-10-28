package org.example.springbatch.batch.processor;

import org.example.springbatch.entity.Customer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class CustomerProcessor implements ItemProcessor<Customer, Customer> {

    @Override
    public Customer process(Customer customer) {
        customer.updateName(UUID.randomUUID().toString());
        return customer;
    }
}
