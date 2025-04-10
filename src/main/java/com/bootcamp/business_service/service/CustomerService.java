package com.bootcamp.business_service.service;

import com.bootcamp.business_service.model.ManagementCustomerRQ;
import com.bootcamp.business_service.model.ManagementCustomerRS;
import reactor.core.publisher.Mono;

public interface CustomerService {

    Mono<ManagementCustomerRS> managementCustomer(Mono<ManagementCustomerRQ> managementCustomerRequest);
}
