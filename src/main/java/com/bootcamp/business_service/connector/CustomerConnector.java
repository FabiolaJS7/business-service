package com.bootcamp.business_service.connector;

import com.bootcamp.business_service.util.JsonTransferUtil;
import com.bootcamp.commons.bean.customers.CustomerRequest;
import com.bootcamp.commons.bean.customers.CustomerResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class CustomerConnector {


    WebClient webClient;

    public CustomerConnector(@Qualifier("webClientCustomerService") WebClient webClient) {
        this.webClient = webClient;
    }

    // Endpoint de customer API para traer todos los customers
    public Flux<CustomerResponse> getAllCustomers() {
        log.info("API getAllCustomers RQ.");
        return webClient.get()
                .uri("/api/customers")
                .retrieve()
                .bodyToFlux(CustomerResponse.class)
                .doOnNext(response -> log.info("API getAllCustomers RS: {}",
                        JsonTransferUtil.objectToJson(response)))
                .doOnError(error -> log.error("API error getAllCustomers: {}", error.getMessage()));


    }

    // Endpoint de customer API para traer customer by id
    public Mono<CustomerResponse> getCustomerById(String customerId) {
        log.info("API getCustomerById RQ: {}", customerId);
        return webClient.get()
                .uri("/api/customers/" + customerId)
                .retrieve()
                .bodyToMono(CustomerResponse.class)
                .doOnNext(response -> log.info("API getCustomerById RS: {}",
                        JsonTransferUtil.objectToJson(response)))
                .doOnError(error -> log.error("API error getCustomerById: {}", error.getMessage()));
    }

    // Endpoint de customer API para crear un customer
    public Mono<CustomerResponse> createCustomer(Mono<CustomerRequest> customerRequest) {
        return customerRequest
                .doOnNext(rq -> log.debug("API createCustomer RQ: {}", JsonTransferUtil.objectToJson(rq)))
                .flatMap(rq -> webClient.post()
                        .uri("/api/customers")
                        .bodyValue(rq)
                        .retrieve()
                        .bodyToMono(CustomerResponse.class)
                        .doOnNext(response -> log.debug("API createCustomer RS: {}",
                                JsonTransferUtil.objectToJson(response)))
                        .doOnError(error -> log.error("API error createCustomer: {}", error.getMessage()))
                );
    }
}
