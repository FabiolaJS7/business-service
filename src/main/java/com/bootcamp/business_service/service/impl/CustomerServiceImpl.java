package com.bootcamp.business_service.service.impl;

import com.bootcamp.business_service.connector.CustomerConnector;
import com.bootcamp.business_service.constants.CasesUpdateConstants;
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
                    if (managementCustomerRQ.getAction().equalsIgnoreCase(CasesUpdateConstants.CREATE_CUSTOMER)) {
                        return customerConnector.createCustomer(Mono.just(CustomerMapperStruct.INSTANCE
                                .toCustomerRequestOfManagementCustomerRq(managementCustomerRQ)));
                    } else {
                        return Mono.just(new CustomerResponse());
                    }
                })
                .map(customerResponse -> {
                    ManagementCustomerRS managementCustomerRS = new ManagementCustomerRS();
                    if (!customerResponse.getId().isEmpty()) {
                        managementCustomerRS.setResult(Boolean.TRUE);
                        managementCustomerRS.setMessage(customerResponse.getId());
                    }
                    return managementCustomerRS;
                })
                .onErrorResume(e -> {
                    log.error("3. Management customer error: {}", e.getMessage());
                    // Devuelve un ManagementCustomerRS en falste en caso de error
                    ManagementCustomerRS emptyResponse = new ManagementCustomerRS();
                    emptyResponse.setResult(Boolean.FALSE);
                    emptyResponse.setMessage("Error occurred while consulting the customer service.");
                    return Mono.just(emptyResponse);
                })
                .doOnSuccess(m -> log.info("2. Management customer successfully: {}", m));
    }
}
