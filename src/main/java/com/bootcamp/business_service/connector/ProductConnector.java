package com.bootcamp.business_service.connector;

import com.bootcamp.business_service.util.JsonTransferUtil;
import com.bootcamp.commons.bean.products.BalanceBeanResponse;
import com.bootcamp.commons.bean.products.BalanceBeanRequest;
import com.bootcamp.commons.bean.products.ProductRequest;
import com.bootcamp.commons.bean.products.ProductResponse;
import com.bootcamp.commons.bean.products.ProductUpdateRQ;
import io.swagger.v3.core.util.Json;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class ProductConnector {

     WebClient webClient;

    public ProductConnector(@Qualifier("webClientProductService") WebClient webClient) {
        this.webClient = webClient;
    }

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

    public Flux<ProductResponse> getProductsByCustomerId(String customerId) {
        log.info("API getProductByCustomerId RQ: {}", customerId);
        return webClient.get()
                .uri("/api/products/customer/" + customerId)
                .retrieve()
                .bodyToFlux(ProductResponse.class)
                .doOnNext(productResponse -> log.info("API getProductByCustomerId RS Successfully"))
                .doOnError(error -> log.error("Error while getting product by customer id {}",
                        error.getMessage()));

    }

    public Mono<ProductResponse> updateProduct(String productId, Mono<ProductUpdateRQ> productUpdateRQ) {
        return webClient.put()
                .uri("/api/products/" + productId)
                .body(productUpdateRQ, ProductUpdateRQ.class)
                .retrieve()
                .bodyToMono(ProductResponse.class)
                .doOnError(error -> log.error("Error while update product: {}", error.getMessage()));
    }

    public Mono<ProductResponse> getProductById(String productId) {
        log.info("API getProductById RQ: {}", productId);
        return webClient.get()
                .uri("/api/products/" + productId)
                .retrieve()
                .bodyToMono(ProductResponse.class)
                .doOnError(error -> log.error("Error while getting product by id: {}", error.getMessage()));
    }

    public Mono<BalanceBeanResponse> findBalanceByProductId(String productId) {
        return webClient.get()
                .uri("/api/products/" + productId + "/balance")
                .retrieve()
                .bodyToMono(BalanceBeanResponse.class)
                .doOnError(error -> log.error("Error while getting balance by product: {}", error.getMessage()));
    }

    public Mono<BalanceBeanResponse> updateBalance(String productId, Mono<BalanceBeanRequest> balanceBeanRequestMono) {
        log.info("API updateBalance RQ: {}", JsonTransferUtil.objectToJson(balanceBeanRequestMono));
        return webClient.put()
                .uri("/api/products/" + productId + "/balance")
                .body(balanceBeanRequestMono, BalanceBeanRequest.class)
                .retrieve()
                .bodyToMono(BalanceBeanResponse.class)
                .doOnNext(rs -> log.info("API updateBalance RS: {}", JsonTransferUtil.objectToJson(rs)))
                .doOnError(error -> log.error("Error API while update balance: {}", error.getMessage()));
    }

    public Mono<ProductResponse> getProductByAccountNumber(String accountNumber) {
        log.info("API getProductByAccountNumber RQ: {}", accountNumber);
        return webClient.get()
                .uri("/api/products/account/" + accountNumber)
                .retrieve()
                .bodyToMono(ProductResponse.class)
                .doOnNext(productResponse -> log.info("API getProductByAccountNumber RS: {} ",
                        JsonTransferUtil.objectToJson(productResponse)))
                .doOnError(error -> log.error("Error while getting product by account: {}", error.getMessage()));
    }

}
