package com.bootcamp.business_service.connector;

import com.bootcamp.commons.bean.products.ProductRequest;
import com.bootcamp.commons.bean.products.ProductResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
                .doOnError(error -> log.error("Error while fetching products: {}", error.getMessage()));


    }

    public Mono<String> createProduct(Mono<ProductRequest> productRequest) {
        return webClient.post()
                .uri("/api/products")
                .body(productRequest, ProductRequest.class) //Enviado productRequest como body
                .retrieve()
                .bodyToMono(String.class)
                .doOnError(error -> log.error("Error while create product: {}", error.getMessage()));

    }



}
