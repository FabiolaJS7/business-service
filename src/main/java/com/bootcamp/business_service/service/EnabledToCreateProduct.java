package com.bootcamp.business_service.service;

import com.bootcamp.business_service.connector.CustomerConnector;
import com.bootcamp.business_service.connector.ProductConnector;
import com.bootcamp.business_service.constants.ProductTypeConstants;
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

                    // si el producto existe, el cliente es PERSONAL y el tipo de producto SA (Cuenta de ahorros), CA (cuenta corriente) o FA (Plazo fijo) no debe permitir la creación
                    if (exists) {
                        if (map.get("customerType").equalsIgnoreCase("P")) {
                            if (ProductTypeConstants.PASSIVE_PRODUCTS.contains(map.get("productType"))) {
                                map.put("enabled", "false");
                                map.put("message", "Customer type P (Personal) has SA (Save Account) yet, can't create this product again");
                            } else if (map.get("productType").equalsIgnoreCase(ProductTypeConstants.CREDIT_PERSONAL)) {
                                map.put("enabled", "false");
                                map.put("message", "Customer type P (Personal) has a Credit personal yet, can't create this product again");
                            }

                        }
                    }

                    if (map.get("customerType").equalsIgnoreCase("B")) {

                        if ((map.get("productType").equalsIgnoreCase(ProductTypeConstants.SAVING_ACCOUNT)
                                || map.get("productType").equalsIgnoreCase(ProductTypeConstants.FIXED_ACCOUNT) )) {
                            map.put("enabled", "false");
                            map.put("message", "Customer type B (Bussines) can't create products SA (Save account) or FA (Fixed account)");
                        } else if (map.get("productType").equalsIgnoreCase(ProductTypeConstants.CREDIT_PERSONAL)) {
                            map.put("enabled", "false");
                            map.put("message", "Customer type B (Bussines) can't create product CP (Credit personal)");
                        }

                    }

                    boolean hasCreditCard = productResponses
                            .stream()
                            .anyMatch(productResponse -> productResponse.getProductType()
                                    .equalsIgnoreCase(ProductTypeConstants.CREDIT_CARD));

                    if (map.get("productType").equalsIgnoreCase(ProductTypeConstants.SAVING_ACCOUNT)
                            && map.get("customerType").equalsIgnoreCase("V")) {
                        if (!hasCreditCard) {
                            map.put("enabled", "false");
                            map.put("message", "Customer type V (Personal VIP) should be credit card previously to create SA (Save account)");
                        }

                    }

                    if (map.get("productType").equalsIgnoreCase(ProductTypeConstants.CURRENT_ACCOUNT)
                            && map.get("customerType").equalsIgnoreCase("M")) {
                        if (!hasCreditCard) {
                            map.put("enabled", "false");
                            map.put("message", "Customer type M (Bussines Pyme) should be credit card previously to create CA (Current account)");
                        }
                    }


                    return Mono.just(map);
                });

    }
}
