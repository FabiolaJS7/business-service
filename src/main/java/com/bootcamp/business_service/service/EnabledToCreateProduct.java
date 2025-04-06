package com.bootcamp.business_service.service;

import com.bootcamp.business_service.connector.CustomerConnector;
import com.bootcamp.business_service.connector.ProductConnector;
import com.bootcamp.business_service.model.CreateProductRQ;
import com.bootcamp.commons.bean.customers.CustomerResponse;
import com.bootcamp.commons.bean.products.ProductResponse;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.HashMap;
import java.util.List;

@Component
@AllArgsConstructor
@Slf4j
public class EnabledToCreateProduct {

    ProductConnector productConnector;
    CustomerConnector customerConnector;

    public Mono<HashMap<String, String>> validate(Mono<CreateProductRQ> createProductRQ) {

        HashMap<String, String> map = new HashMap<>();
        map.put("enabled", "true");

        Mono<CustomerResponse> customerResponseFound = createProductRQ
                .flatMap(createProductRQ1 -> customerConnector.getCustomerById(createProductRQ1.getCustomerId()))
                .subscribeOn(Schedulers.parallel());

        Flux<ProductResponse> productResponseFlux = createProductRQ.map(CreateProductRQ::getCustomerId)
                .flatMapMany(s -> productConnector.getProductsByCustomerId(s))
                .subscribeOn(Schedulers.parallel());


        return Mono.zip(productResponseFlux.collectList(), customerResponseFound, createProductRQ)
                .flatMap(tuple -> {
                    List<ProductResponse> productResponses = tuple.getT1(); // Lista de productos
                    CustomerResponse customerResponse = tuple.getT2(); // Respuesta del cliente
                    CreateProductRQ createProductRQ1 = tuple.getT3();

                    map.put("customerType", customerResponse.getTypeClient());
                    map.put("productType", createProductRQ1.getProductType());

                    // Si el tipo de producto que se quiere crear ya existe para el cliente
                    boolean exists = productResponses.stream()
                            .anyMatch(productResponse -> productResponse.getProductType()
                                    .equals(createProductRQ1.getProductType()));

                    // si el producto existe, el cliente es PERSONAL y el tipo de producto SA (Cuenta de ahorros) no debe permitir la creación
                    if (exists && map.get("customerType").equalsIgnoreCase("P") && map.get("productType").equalsIgnoreCase("SA")) {
                        map.put("enabled", "false");
                        map.put("message", "Customer type P (Personal) has SA (Save Account) yet, can't create this product again");
                    }

                    return Mono.just(map);
                });

    }
}
