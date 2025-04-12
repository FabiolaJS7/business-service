package com.bootcamp.business_service.service.impl;

import com.bootcamp.business_service.connector.CustomerConnector;
import com.bootcamp.business_service.constants.ActionCustomerConstants;
import com.bootcamp.business_service.mapper.CustomerMapperStruct;
import com.bootcamp.business_service.model.ManagementCustomerRQ;
import com.bootcamp.business_service.model.ManagementCustomerRS;
import com.bootcamp.business_service.service.CustomerService;
import com.bootcamp.commons.bean.customers.CustomerResponse;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@Slf4j
@AllArgsConstructor
public class CustomerServiceImpl implements CustomerService {

    CustomerConnector customerConnector;

    @Override
    public Mono<ManagementCustomerRS> managementCustomer(Mono<ManagementCustomerRQ> customerRequest) {
        return customerRequest
                .doOnNext(c -> log.info("1. Management customer customerRequest: {}", c))
                .flatMap(managementCustomerRQ -> {
                    if (managementCustomerRQ.getAction().equalsIgnoreCase(ActionCustomerConstants.CREATE_CUSTOMER)) {
                        return customerConnector.createCustomer(Mono.just(CustomerMapperStruct.INSTANCE
                                .toCustomerRequestOfManagementCustomerRq(managementCustomerRQ)));
                    } else {
                        return Mono.just(new CustomerResponse());
                    }
                })
                .map(customerResponse -> {
                    ManagementCustomerRS managementCustomerRS = new ManagementCustomerRS();
                    managementCustomerRS.setResult(customerResponse.getId().isEmpty() ? Boolean.FALSE : Boolean.TRUE);
                    managementCustomerRS.setMessage(customerResponse.getId());
                    return managementCustomerRS;
                })
                .doOnSuccess(m -> log.info("2. Management customer successfully: {}", m))
                .doOnError(e -> log.error("3. Management customer error: {}", e.getMessage()));

    }
}
