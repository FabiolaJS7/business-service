package com.bootcamp.business_service.components;

import com.bootcamp.business_service.connector.CustomerConnector;
import com.bootcamp.business_service.connector.ProductConnector;
import com.bootcamp.business_service.model.CreateProductRQ;
import com.bootcamp.business_service.util.JsonTransferUtil;
import com.bootcamp.commons.bean.customers.CustomerResponse;
import com.bootcamp.commons.bean.products.ProductResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


@ExtendWith(MockitoExtension.class)
class LogicalCreateProductTest {

    @Mock
    ProductConnector productConnector;

    @Mock
    CustomerConnector customerConnector;

    @InjectMocks
    LogicalCreateProduct logicalCreateProduct;

    @Test
    void shouldCreateProductPassiveSavingAccount_whenCustomerTypeIsPersonal_thenMapEnabledIsTrue() {
        //Arr
        CreateProductRQ createProductRQ = new CreateProductRQ();
        createProductRQ.setCustomerId("67ec1a41fabf6d0f4ada9865");
        createProductRQ.setProductType("SA"); //Saving Account (Cuenta de ahorros)
        createProductRQ.setOpenAmount(100.00);
        createProductRQ.setFamilyProduct("PASSIVE");
        createProductRQ.setUserBank("admin@bank.com");

        CustomerResponse customerResponse = JsonTransferUtil.getObjectFromJSONFile(CustomerResponse.class,
                "CustomerResponse.json");
        List<ProductResponse> products = Arrays.asList(JsonTransferUtil.getObjectFromJSONFile(ProductResponse[].class,
                "ProductResponse.json"));

        HashMap<String, String> map = new HashMap<>();
        map.put("enabled", "true");

        Mockito.when(customerConnector.getCustomerById(createProductRQ.getCustomerId())).
                thenReturn(Mono.just(customerResponse));
        Mockito.when(productConnector.getProductsByCustomerId(createProductRQ.getCustomerId())).
                thenReturn((Flux.fromIterable(products)));


        Mono<HashMap<String, String>> resultValidate = logicalCreateProduct.validate(Mono.just(createProductRQ));

        StepVerifier
                .create(resultValidate)
                .expectNextMatches(stringStringHashMap -> stringStringHashMap.get("message")
                        .equals("Customer type P (Personal) has SA (Save Account) yet, can't create this product again"))
                .expectComplete()
                .verify();
    }

    @Test
    void shouldNotCreateProductSavingAccount_whenCustomerTypePersonalHaveSavingAccountYet_thenMapEnabledIsFalse() {
        //Arr
        CreateProductRQ createProductRQ = new CreateProductRQ();
        createProductRQ.setProductType("SA"); //Saving Account (Cuenta de ahorros)
        createProductRQ.setOpenAmount(100.00);
        createProductRQ.setFamilyProduct("PASSIVE");
        createProductRQ.setUserBank("admin@bank.com");

        CustomerResponse customerResponse = JsonTransferUtil.getObjectFromJSONFile(CustomerResponse.class,
                "CustomerResponse.json");
        List<ProductResponse> products = Arrays.asList(JsonTransferUtil.getObjectFromJSONFile(ProductResponse[].class,
                "ProductResponse.json"));

        HashMap<String, String> map = new HashMap<>();
        map.put("enabled", "true");

        Mockito.when(customerConnector.getCustomerById(createProductRQ.getCustomerId())).
                thenReturn(Mono.just(customerResponse));
        Mockito.when(productConnector.getProductsByCustomerId(createProductRQ.getCustomerId())).
                thenReturn((Flux.fromIterable(products)));


        Mono<HashMap<String, String>> resultValidate = logicalCreateProduct.validate(Mono.just(createProductRQ));

        StepVerifier
                .create(resultValidate)
                .expectNextMatches(stringStringHashMap -> stringStringHashMap.get("message")
                        .equals("Customer type P (Personal) has SA (Save Account) yet, can't create this product again"))
                .expectComplete()
                .verify();
    }
}