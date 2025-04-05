package com.bootcamp.business_service.service;

import com.bootcamp.commons.bean.customers.CustomerResponse;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Profile;
import reactor.core.publisher.Flux;

@SpringBootTest
@Profile("local")
@Slf4j
class ClientCustomerServiceImplTest {

    @Autowired
    ClientCustomerServiceImpl clientCustomerService;

    @Test
    void getAllProducts() {
        Flux<CustomerResponse> customers = clientCustomerService.getAllCustomers();
        customers.subscribe();

        System.out.println(customers.collectList().block());



    }
}