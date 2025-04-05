package com.bootcamp.business_service.connector;

import com.bootcamp.business_service.util.JsonTransferUtil;
import com.bootcamp.commons.bean.products.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Profile;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

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

    @Test
    void shouldCreateProduct_whenProductRequestIsValid() {

        Mono<String> result = productConnector.createProduct(Mono.just(getProductRequest()));
        System.out.println("result create product: " + result);
    }

    private ProductRequest getProductRequest() {
        ProductRequest productRequest = new ProductRequest();
        productRequest.setProductType("SA");
        productRequest.setFamilyProduct("PASSIVE");
        CustomerBean customerBean = new CustomerBean();
        customerBean.setCustomerId("67f00c6fd214c769e74425a7");
        customerBean.setCustomerType("P");
        productRequest.setCustomer(customerBean);

        PassiveProductBean passiveProductBean = new PassiveProductBean();
        passiveProductBean.setIsFreeCommission(false);
        passiveProductBean.setAmountOfOpen(10.0);
        passiveProductBean.setAccountNumber("4654654-65465-654-65465");
        InfoTransactionBean infoTransactionBean = new InfoTransactionBean();
        infoTransactionBean.setCommission(10.00);
        infoTransactionBean.setMaxPerMonth(String.valueOf(15));
        infoTransactionBean.setTransactionDone(String.valueOf(20));
        infoTransactionBean.setEnabledToMovement(true);
        passiveProductBean.setInforToTransaction(infoTransactionBean);
        productRequest.setPassiveProduct(passiveProductBean);

        AdditionalPersonBean additionalPersonBean = new AdditionalPersonBean();
        additionalPersonBean.setEmail("test@test.com");
        additionalPersonBean.setPhone("123456789");
        additionalPersonBean.setFullName("Test Person");

        IdentificationBean identificationBean = new IdentificationBean();
        identificationBean.setNumberIdentification("654654");
        identificationBean.setTypeIdentification("DNI");
        additionalPersonBean.setIdentification(identificationBean);

        List<AdditionalPersonBean> holders = new ArrayList<>();
        holders.add(additionalPersonBean);
        //productRequest.setHolders(holders);
        productRequest.setHolders(new ArrayList<>());

        List<AdditionalPersonBean> auth = new ArrayList<>();
        auth.add(additionalPersonBean);
        //productRequest.setAuthorizedSignatories(auth);
        productRequest.setAuthorizedSignatories(new ArrayList<>());

        productRequest.setUserBank("admin");

        return productRequest;
    }
}