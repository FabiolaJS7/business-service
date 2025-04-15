package com.bootcamp.business_service.service;

import com.bootcamp.business_service.model.ProductTypeRQ;
import com.bootcamp.business_service.model.ProductTypeRS;
import reactor.core.publisher.Mono;

public interface ProductTypeService {
    Mono<ProductTypeRS> createProductType (Mono<ProductTypeRQ> productTypeRQ);
}
