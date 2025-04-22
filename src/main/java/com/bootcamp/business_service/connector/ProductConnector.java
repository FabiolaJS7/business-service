package com.bootcamp.business_service.connector;

import com.bootcamp.business_service.util.JsonTransferUtil;
import com.bootcamp.commons.bean.products.*;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class ProductConnector {

    public static final String MAIN_PATH_PRODUCT = "/api/products/";
    WebClient webClient;

    public ProductConnector(@Qualifier("webClientService") WebClient webClient) {
        this.webClient = webClient;
    }

    // Endpoint de product API crear productos
    @CircuitBreaker(name = "productService", fallbackMethod = "fallbackCreateProduct")
    public Mono<String> createProduct(Mono<ProductRequest> productRequest) {
        return productRequest
                .doOnNext(rq -> log.info("API createProduct RQ:  {}", JsonTransferUtil.objectToJson(rq)))
                .flatMap(rq ->  webClient.post()
                        .uri("/api/products")
                        .bodyValue(rq) //Enviado productRequest como body
                        .retrieve()
                        .bodyToMono(String.class)
                        .doOnNext(s -> log.info("API createProduct RS:  {}", JsonTransferUtil.objectToJson(s)))
                        .doOnError(error -> log.error("API error create product: {}", error.getMessage())));
    }

    // Endpoint de product API obtener products por customer id
    @CircuitBreaker(name = "productService", fallbackMethod = "fallbackGetProductsByCustomerId")
    public Flux<ProductResponse> getProductsByCustomerId(String customerId) {
        log.info("API getProductByCustomerId RQ: {}", customerId);
        return webClient.get()
                .uri(MAIN_PATH_PRODUCT + "customer/" + customerId)
                .retrieve()
                .bodyToFlux(ProductResponse.class)
                .doOnNext(productResponse -> log.info("API getProductByCustomerId RS {}",
                        JsonTransferUtil.objectToJson(productResponse)))
                .doOnError(error -> log.error("Error API while getProductByCustomerId id {}",
                        error.getMessage()));

    }

    // Endpoint para actualizar producto
    @CircuitBreaker(name = "productService", fallbackMethod = "fallbackUpdateProduct")
    public Mono<ProductResponse> updateProduct(String productId, Mono<ProductUpdateRQ> productUpdateRQ) {
        return productUpdateRQ
                .doOnNext(rq -> log.info("API updateProduct RQ: {}", JsonTransferUtil.objectToJson(rq)))
                .flatMap(rq -> webClient.put()
                        .uri(MAIN_PATH_PRODUCT + productId)
                        .body(productUpdateRQ, ProductUpdateRQ.class)
                        .retrieve()
                        .bodyToMono(ProductResponse.class)
                        .doOnNext(response -> log.info("API updateProduct RS: {}",
                                JsonTransferUtil.objectToJson(response)))
                        .doOnError(error -> log.error("Error API while update product: {}", error.getMessage()))
                );
    }

    // Endpoint de product API para obtener producto por id
    @CircuitBreaker(name = "productService", fallbackMethod = "fallbackGetProductById")
    public Mono<ProductResponse> getProductById(String productId) {
        log.info("API getProductById RQ: {}", productId);
        return webClient.get()
                .uri(MAIN_PATH_PRODUCT + productId)
                .retrieve()
                .bodyToMono(ProductResponse.class)
                .doOnNext(response -> log.info("API getProductById RS: {}",
                        JsonTransferUtil.objectToJson(response)))
                .doOnError(error -> log.error("Error API while getting product by id: {}", error.getMessage()));
    }

    // Endpoint de product API para encontrar el balance de un producto por id
    @CircuitBreaker(name = "productService", fallbackMethod = "fallbackFindBalanceByProductId")
    public Mono<BalanceBeanResponse> findBalanceByProductId(String productId) {
        log.info("API findBalanceByProductId RQ: {}", productId);
        return webClient.get()
                .uri(MAIN_PATH_PRODUCT + productId + "/balance")
                .retrieve()
                .bodyToMono(BalanceBeanResponse.class)
                .doOnNext(balanceBeanResponse -> log.info("API findBalanceByProductId RS: {}",
                        JsonTransferUtil.objectToJson(balanceBeanResponse)))
                .doOnError(error -> log.error("Error API while getting balance by product: {}", error.getMessage()));
    }

    // Endpoint de product API para actualizar el balance de un producto
    @CircuitBreaker(name = "productService", fallbackMethod = "fallbackUpdateBalance")
    public Mono<BalanceBeanResponse> updateBalance(String productId, Mono<BalanceBeanRequest> balanceBeanRequestMono) {
        log.info("API updateBalance RQ: {}", JsonTransferUtil.objectToJson(balanceBeanRequestMono));
        return webClient.put()
                .uri(MAIN_PATH_PRODUCT + productId + "/balance")
                .body(balanceBeanRequestMono, BalanceBeanRequest.class)
                .retrieve()
                .bodyToMono(BalanceBeanResponse.class)
                .doOnNext(rs -> log.info("API updateBalance RS: {}",
                        JsonTransferUtil.objectToJson(rs)))
                .doOnError(error -> log.error("Error API while update balance: {}", error.getMessage()));
    }

    // Endpoint de product API para obtener product por numero de cuenta
    @CircuitBreaker(name = "productService", fallbackMethod = "fallbackGetProductByAccountNumber")
    public Mono<ProductResponse> getProductByAccountNumber(String accountNumber) {
        log.info("API getProductByAccountNumber RQ: {}", accountNumber);
        return webClient.get()
                .uri(MAIN_PATH_PRODUCT + "account/" + accountNumber)
                .retrieve()
                .bodyToMono(ProductResponse.class)
                .doOnNext(productResponse -> log.info("API getProductByAccountNumber RS: {} ",
                        JsonTransferUtil.objectToJson(productResponse)))
                .doOnError(error -> log.error("Error API while getting product by account: {}", error.getMessage()));
    }

    private Mono<String> fallbackCreateProduct(Mono<ProductRequest> productRequest, Throwable throwable) {
        log.error("Fallback for createProduct {}, {}:", JsonTransferUtil.objectToJson(productRequest),
                throwable.getMessage());
        return Mono.just("Fallback for createProduct");
    }

    private Flux<ProductResponse> fallbackGetProductsByCustomerId(String customerId, Throwable throwable) {
        log.error("Fallback for getProductsByCustomerId {}, {}", customerId, throwable.getMessage());
        return Flux.empty();
    }

    private Mono<ProductResponse> fallbackUpdateProduct(String productId, Mono<ProductUpdateRQ> productUpdateRQ,
                                                Throwable throwable) {
        log.error("Fallback for updateProduct {},{}, {}", productId, JsonTransferUtil.objectToJson(productUpdateRQ),
                throwable.getMessage());
        return Mono.just(new ProductResponse());
    }

    private Mono<ProductResponse> fallbackGetProductById(String productId, Throwable throwable) {
        log.error("Fallback for getProductById {}, {}", productId, throwable.getMessage());
        return Mono.just(new ProductResponse());
    }

    private Mono<BalanceBeanResponse> fallbackFindBalanceByProductId(String productId, Throwable throwable) {
        log.error("Fallback for findBalanceByProductId {}, {}", productId, throwable.getMessage());
        return Mono.just(new BalanceBeanResponse());
    }

    private Mono<BalanceBeanResponse> fallbackUpdateBalance(String productId,
                                                            Mono<BalanceBeanRequest> balanceBeanRequestMono, Throwable throwable) {
        log.error("Fallback for updateBalance {}, {}, {}", productId,
                JsonTransferUtil.objectToJson(balanceBeanRequestMono), throwable.getMessage());
        return Mono.just(new BalanceBeanResponse());
    }

    private Mono<ProductResponse> fallbackGetProductByAccountNumber(String accountNumber, Throwable throwable) {
        log.info("Fallback for getProductByAccountNumber: {} , {}", accountNumber, throwable.getMessage());
        return Mono.just(new ProductResponse());
    }

}
