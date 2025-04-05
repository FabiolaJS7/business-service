package com.bootcamp.business_service.connector;

import com.bootcamp.commons.bean.customers.CustomerResponse;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;

@SpringBootTest
@Slf4j
class CustomerConnectorTest {

    @Autowired
    CustomerConnector clientCustomerService;

    @Disabled
    @Test
    void shouldGetCustomers() {
        Flux<CustomerResponse> customers = clientCustomerService.getAllCustomers();
        customers.subscribe();

        System.out.println(customers.collectList().block());



    }
}