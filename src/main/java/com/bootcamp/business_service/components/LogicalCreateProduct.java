package com.bootcamp.business_service.components;

import com.bootcamp.business_service.connector.CustomerConnector;
import com.bootcamp.business_service.connector.ProductConnector;
import com.bootcamp.business_service.constants.CustomerTypeConstants;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Component
@AllArgsConstructor
@Slf4j
public class LogicalCreateProduct {

    private static final String ENABLED = "enabled";
    private static final String MESSAGE = "message";
    private static final String CUSTOMER_TYPE = "customerType";
    private static final String PRODUCT_TYPE = "productType";

    ProductConnector productConnector;
    CustomerConnector customerConnector;

    public Mono<HashMap<String, String>> validate(Mono<CreateProductRQ> createProductRQ) {

        HashMap<String, String> map = new HashMap<>();
        map.put(ENABLED, Boolean.TRUE.toString());

        Mono<CustomerResponse> customerResponseFound = createProductRQ
                .flatMap(createProductRQ1 -> customerConnector.getCustomerById(createProductRQ1.getCustomerId()))
                .subscribeOn(Schedulers.parallel());

        Flux<ProductResponse> productResponseFlux = createProductRQ.map(CreateProductRQ::getCustomerId)
                .flatMapMany(s -> productConnector.getProductsByCustomerId(s))
                .subscribeOn(Schedulers.parallel());


        return Mono.zip(customerResponseFound, productResponseFlux.collectList().defaultIfEmpty(new ArrayList<>()), createProductRQ)
                .flatMap(tuple -> {
                    CustomerResponse customerResponse = tuple.getT1();
                    List<ProductResponse> productResponses = tuple.getT2();
                    CreateProductRQ createProductRQ1 = tuple.getT3();

                    map.put(CUSTOMER_TYPE, customerResponse.getTypeClient());
                    map.put(PRODUCT_TYPE, createProductRQ1.getProductType());

                    if (isProductAlreadyExists(productResponses, createProductRQ1.getProductType())) {
                        validatePersonalCustomer(map);
                        validateBusinessCustomer(map);
                    }

                    validateVipCustomer(map, productResponses);
                    validatePymeCustomer(map, productResponses);

                    return Mono.just(map);
                })
                .onErrorResume(throwable -> {
                    map.put(ENABLED, "false");
                    map.put(MESSAGE, "Error occurred while consulting some service.");
                    return Mono.just(map);
                })
                .doOnSuccess(response -> log.info("Validations to create product: {}", response));


    }

    // Método para verificar si el producto ya existe
    private boolean isProductAlreadyExists(List<ProductResponse> productResponses, String productType) {
        return productResponses.stream()
                .anyMatch(productResponse -> productResponse.getProductType().equals(productType));
    }

    // Validaciones para clientes tipo P (Personal)
    private void validatePersonalCustomer(HashMap<String, String> map) {
        if (map.get(CUSTOMER_TYPE).equalsIgnoreCase(CustomerTypeConstants.PERSONAL)) {
            if (ProductTypeConstants.PASSIVE_PRODUCTS.contains(map.get(PRODUCT_TYPE))) {
                map.put(ENABLED, Boolean.FALSE.toString());
                map.put(MESSAGE, "Customer type P (Personal) has SA (Save Account) yet, can't create this product again");
            } else if (map.get(PRODUCT_TYPE).equalsIgnoreCase(ProductTypeConstants.CREDIT_PERSONAL)) {
                map.put(ENABLED, Boolean.FALSE.toString());
                map.put(MESSAGE, "Customer type P (Personal) has a Credit personal yet, can't create this product again");
            }
        }
    }

    // Validaciones para clientes tipo B (Business)
    private void validateBusinessCustomer(HashMap<String, String> map) {
        if (map.get(CUSTOMER_TYPE).equalsIgnoreCase(CustomerTypeConstants.BUSINESS)) {
            if (map.get(PRODUCT_TYPE).equalsIgnoreCase(ProductTypeConstants.SAVING_ACCOUNT)
                    || map.get(PRODUCT_TYPE).equalsIgnoreCase(ProductTypeConstants.FIXED_ACCOUNT)) {
                map.put(ENABLED, Boolean.FALSE.toString());
                map.put(MESSAGE, "Customer type B (Business) can't create products SA (Save account) or FA (Fixed account)");
            } else if (map.get(PRODUCT_TYPE).equalsIgnoreCase(ProductTypeConstants.CREDIT_PERSONAL)) {
                map.put(ENABLED, Boolean.FALSE.toString());
                map.put(MESSAGE, "Customer type B (Business) can't create product CP (Credit personal)");
            }
        }
    }

    // Validaciones para clientes tipo V (Personal VIP)
    private void validateVipCustomer(HashMap<String, String> map, List<ProductResponse> productResponses) {
        boolean hasCreditCard = productResponses.stream()
                .anyMatch(productResponse -> productResponse.getProductType().equalsIgnoreCase(ProductTypeConstants.CREDIT_CARD));

        if (map.get(PRODUCT_TYPE).equalsIgnoreCase(ProductTypeConstants.SAVING_ACCOUNT)
                && map.get(CUSTOMER_TYPE).equalsIgnoreCase(CustomerTypeConstants.PERSONAL_VIP)) {
            if (!hasCreditCard) {
                map.put(ENABLED, Boolean.FALSE.toString());
                map.put(MESSAGE, "Customer type V (Personal VIP) should have a credit card previously to create SA (Save account)");
            }
        }
    }

    // Validaciones para clientes tipo M (Business Pyme)
    private void validatePymeCustomer(HashMap<String, String> map, List<ProductResponse> productResponses) {
        boolean hasCreditCard = productResponses.stream()
                .anyMatch(productResponse -> productResponse.getProductType().equalsIgnoreCase(ProductTypeConstants.CREDIT_CARD));

        if (map.get(PRODUCT_TYPE).equalsIgnoreCase(ProductTypeConstants.CURRENT_ACCOUNT)
                && map.get(CUSTOMER_TYPE).equalsIgnoreCase(CustomerTypeConstants.BUSINESS_PYME)) {
            if (!hasCreditCard) {
                map.put(ENABLED, Boolean.FALSE.toString());
                map.put(MESSAGE, "Customer type M (Business Pyme) should have a credit card previously to create CA (Current account)");
            }
        }
    }
}
