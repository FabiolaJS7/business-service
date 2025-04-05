package com.bootcamp.business_service.connector;

import com.bootcamp.commons.bean.customers.CustomerResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

@Service
@Slf4j
public class CustomerConnector {

    private final WebClient webClient;

    public CustomerConnector(@Qualifier("webClientCustomerService") WebClient webClient) {
        this.webClient = webClient;
    }

    // Endpoint de la API para traer todos los customers
    public Flux<CustomerResponse> getAllCustomers() {
        return webClient.get()
                .uri("/api/customers")
                .retrieve()
                .bodyToFlux(CustomerResponse.class)
                .doOnError(error -> log.error("Error while fetching customers: {}", error.getMessage())); // Log de errores


    }
}
