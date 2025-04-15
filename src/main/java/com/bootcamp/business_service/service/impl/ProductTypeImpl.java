package com.bootcamp.business_service.service.impl;

import com.bootcamp.business_service.model.ProductTypeRQ;
import com.bootcamp.business_service.model.ProductTypeRS;
import com.bootcamp.business_service.service.ProductTypeService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class ProductTypeImpl implements ProductTypeService {
    @Override
    public Mono<ProductTypeRS> createProductType(Mono<ProductTypeRQ> productTypeRQ) {
        return null;
    }
}
