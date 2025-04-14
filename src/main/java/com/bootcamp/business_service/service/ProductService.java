package com.bootcamp.business_service.service;

import com.bootcamp.business_service.model.*;
import reactor.core.publisher.Mono;

public interface ProductService {

    Mono<CreateProductRS> createProduct(Mono<CreateProductRQ> createProductRQ);
    Mono<AdditionalPersonRS> updateAdditionalPerson(Mono<AdditionalPersonRQ> additionalPersonRQ);
    Mono<CreateCardRS> createCardToPassiveProduct(Mono<CreateCardRQ> createCardRQ);
}
