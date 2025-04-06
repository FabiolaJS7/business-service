package com.bootcamp.business_service.expose;

import com.bootcamp.business_service.api.ApiApiDelegate;
import com.bootcamp.business_service.model.AdditionalPersonRQ;
import com.bootcamp.business_service.model.AdditionalPersonRS;
import com.bootcamp.business_service.model.CreateProductRQ;
import com.bootcamp.business_service.model.CreateProductRS;
import com.bootcamp.business_service.service.ProductService;
import com.bootcamp.business_service.util.JsonTransferUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Service
@Slf4j
@AllArgsConstructor
public class BusinessDelegateImpl implements ApiApiDelegate {

    ProductService productService;

    @Override
    public Mono<ResponseEntity<CreateProductRS>> createProduct(Mono<CreateProductRQ> createProductRQ,
                                                                ServerWebExchange exchange) {
        return createProductRQ
                .doOnNext(c -> log.info("-> Init createProduct: {}", JsonTransferUtil.objectToJson(c)))
                .flatMap(c -> productService.createProduct(Mono.just(c)))
                .map(ResponseEntity::ok)
                .doOnSuccess(createProductRS -> log.info("success createProduct: {}",
                        JsonTransferUtil.objectToJson(createProductRS)))
                .doOnError(throwable -> log.error("Request error createProduct {}", throwable.getMessage()))
                .onErrorResume(e -> Mono.just(new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR)));

    }

    @Override
    public Mono<ResponseEntity<AdditionalPersonRS>> additionalPerson(Mono<AdditionalPersonRQ> additionalPersonRQ,
                                                                      ServerWebExchange exchange) {
        return additionalPersonRQ
                .doOnNext(a -> log.info("-> Init additionalPerson: {}", JsonTransferUtil.objectToJson(a)))
                .flatMap(a -> productService.updateAdditionalPerson(Mono.just(a)))
                .map(ResponseEntity::ok)
                .doOnSuccess(a -> log.info("success update additionalperson {}", JsonTransferUtil.objectToJson(a)))
                .doOnError(throwable -> log.error("Request error updateAdditionalPerson {}", throwable.getMessage()))
                .onErrorResume(e -> Mono.just(new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR)));

    }


}
