package com.bootcamp.business_service.expose;

import com.bootcamp.service.transaction.api.ApiApiDelegate;
import com.bootcamp.service.transaction.model.CreateProductRQ;
import com.bootcamp.service.transaction.model.CreateProductRS;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Service
public class BusinessDelegateImpl implements ApiApiDelegate {

    @Override
    public Mono<ResponseEntity<CreateProductRS>> createProduct(Mono<CreateProductRQ> createProductRQ,
                                                                ServerWebExchange exchange) {
        return Mono.empty();
    }

}
