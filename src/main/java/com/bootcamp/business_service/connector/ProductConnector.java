package com.bootcamp.business_service.connector;

import com.bootcamp.commons.bean.customers.CustomerResponse;
import com.bootcamp.commons.bean.products.ProductResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

@Service
@Slf4j
public class ProductConnector {

    private final WebClient webClient;

    public ProductConnector(@Qualifier("webClientProductService") WebClient webClient) {
        this.webClient = webClient;
    }

    // Endpoint de la API para traer todos los productos
    public Flux<ProductResponse> getAllCustomers() {
        return webClient.get()
                .uri("/api/products")
                .retrieve()
                .bodyToFlux(ProductResponse.class)
                .doOnError(error -> log.error("Error while fetching products: {}", error.getMessage())); // Log de errores


    }



}
