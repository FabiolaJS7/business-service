package com.bootcamp.business_service.service;

import com.bootcamp.commons.bean.customers.CustomerResponse;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

@Service
public class ClientCustomerServiceImpl {

    private final WebClient webClient;

    public ClientCustomerServiceImpl(WebClient webClient) {
        this.webClient = webClient;
    }

    // Endpoint de la API para traer todos los customers
    public Flux<CustomerResponse> getAllCustomers() {
        return webClient.get()
                .uri("/api/customers")
                .retrieve()
                .bodyToFlux(CustomerResponse.class);
    }
}
