package com.bootcamp.business_service.connector;

import com.bootcamp.business_service.util.JsonTransferUtil;
import com.bootcamp.commons.bean.products.ProductResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Profile;
import reactor.core.publisher.Flux;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Profile("local")
class ProductConnectorTest {

    @Autowired
    ProductConnector productConnector;

    @Test
    void getAllProducts() {
    Flux<ProductResponse> productResponseFlux = productConnector.getAllCustomers();
        System.out.println(JsonTransferUtil.objectToJson(productResponseFlux.collectList().block()));
    }
}