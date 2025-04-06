package com.bootcamp.business_service.components;

import com.bootcamp.business_service.connector.CustomerConnector;
import com.bootcamp.business_service.connector.ProductConnector;
import com.bootcamp.business_service.model.AdditionalPersonRQ;
import com.bootcamp.commons.bean.customers.CustomerResponse;
import com.bootcamp.commons.bean.products.ProductResponse;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Component
@AllArgsConstructor
@Slf4j
public class LogicalAddPersonToProduct {

    CustomerConnector customerConnector;
    ProductConnector productConnector;

    public Mono<Boolean> validate(Mono<AdditionalPersonRQ> additionalPersonRQMono) {

        Mono<CustomerResponse> customerResponseFound = additionalPersonRQMono
                .flatMap(createProductRQ1 -> customerConnector.getCustomerById(createProductRQ1.getCustomerId()))
                .subscribeOn(Schedulers.parallel());

        Mono<ProductResponse> productResponseFound = additionalPersonRQMono
                .flatMap(additionalPersonRQ -> productConnector.getProductById(additionalPersonRQ.getProductId()))
                .subscribeOn(Schedulers.parallel());

        return Mono.zip(customerResponseFound, productResponseFound)
                .flatMap(tuple -> {
                    CustomerResponse customerResponse = tuple.getT1();
                    ProductResponse productResponse = tuple.getT2();

                    return additionalPersonRQMono
                            .flatMap(additionalPersonRQ -> {
                                if (additionalPersonRQ.getTypeAdditional().equalsIgnoreCase("HOLD")) {
                                    return Mono.just(productResponse.getHolders()
                                            .stream()
                                            .anyMatch(additionalPersonBean -> additionalPersonBean.getIdentification()
                                                    .getNumberIdentification().equalsIgnoreCase(additionalPersonRQ.getIdentificationNum())));
                                } else {
                                    return Mono.just(productResponse.getAuthorizedSignatories()
                                            .stream()
                                            .anyMatch(additionalPersonBean -> additionalPersonBean.getIdentification()
                                                    .getNumberIdentification().equalsIgnoreCase(additionalPersonRQ.getIdentificationNum())));
                                }
                            }).flatMap(exists -> exists ? Mono.just(false) : Mono.just(true));

                });


    }
}
