package com.bootcamp.business_service.service;

import com.bootcamp.business_service.model.CreateProductRQ;
import com.bootcamp.business_service.model.CreateProductRS;
import reactor.core.publisher.Mono;

public interface ProductService {

    Mono<CreateProductRS> createProduct(Mono<CreateProductRQ> createProductRQ);
}
