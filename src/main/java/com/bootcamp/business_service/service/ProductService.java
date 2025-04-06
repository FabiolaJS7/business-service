package com.bootcamp.business_service.service;

import com.bootcamp.business_service.model.AdditionalPersonRQ;
import com.bootcamp.business_service.model.AdditionalPersonRS;
import com.bootcamp.business_service.model.CreateProductRQ;
import com.bootcamp.business_service.model.CreateProductRS;
import reactor.core.publisher.Mono;

public interface ProductService {

    Mono<CreateProductRS> createProduct(Mono<CreateProductRQ> createProductRQ);
    Mono<AdditionalPersonRS> updateAdditionalPerson(Mono<AdditionalPersonRQ> additionalPersonRQ);
}
