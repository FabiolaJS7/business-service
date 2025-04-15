package com.bootcamp.business_service.connector;

import com.bootcamp.business_service.util.JsonTransferUtil;
import com.bootcamp.commons.bean.products.ProductTypeRequest;
import com.bootcamp.commons.bean.products.ProductTypeResponse;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class ProductTypeConnector {

    WebClient webClient;

    public ProductTypeConnector(@Qualifier("webClientProductService") WebClient webClient) {
        this.webClient = webClient;
    }

    // Endpoint de product API para crear un nuevo tipo de producto
    @CircuitBreaker(name = "productTypeService", fallbackMethod = "fallbackCreateProductType")
    public Mono<ProductTypeResponse> createProductType(Mono<ProductTypeRequest> productTypeRequest) {
        return productTypeRequest
                .doOnNext(rq -> log.info("API createProductType RQ: {}",
                        JsonTransferUtil.objectToJson(rq)))
                .flatMap(rq -> webClient.post()
                        .uri("/api/products/types")
                        .bodyValue(rq)
                        .retrieve()
                        .bodyToMono(ProductTypeResponse.class)
                        .doOnNext(productTypeResponse -> log.info("API createProductType RS {}",
                                JsonTransferUtil.objectToJson(productTypeResponse)))
                        .doOnError(error -> log.error("API error createProductType: {}", error.getMessage()))
                );
    }

    // Endpoint de product API para obtener detalles de los tipos de productos en el banco
    @CircuitBreaker(name = "productTypeService", fallbackMethod = "fallbackGetProductTypeByCode")
    public Mono<ProductTypeResponse> getProductTypeByCode(String productTypeCode) {
        log.info("API getProductByCustomerId RQ: {}", productTypeCode);
        return webClient.get()
                .uri("/api/products/types/" + productTypeCode)
                .retrieve()
                .bodyToMono(ProductTypeResponse.class)
                .doOnNext(productTypeResponse -> log.info("API getProductTypeByCode RQ: {}",
                        JsonTransferUtil.objectToJson(productTypeResponse)))
                .doOnError(throwable -> log.error("API error getProductTypeByCode: {}",
                        throwable.getMessage()));
    }

    // El fallback que devuelve objeto vacíos
    private Mono<ProductTypeResponse> fallbackGetProductTypeByCode(String productTypeCode, Throwable throwable) {
        log.error("Fallback for getProductTypeByCode triggered for productTypeCode {}: {}", productTypeCode,
                throwable.getMessage());
        return Mono.just(new ProductTypeResponse());
    }

    private Mono<ProductTypeResponse> fallbackCreateProductType(Mono<ProductTypeRequest> productTypeRequest,
                                                                Throwable throwable) {
        log.error("Fallback for createProductType triggered for productTypeRequest {}: {}",
                JsonTransferUtil.objectToJson(productTypeRequest), throwable.getMessage());
        return Mono.just(new ProductTypeResponse());
    }
}
