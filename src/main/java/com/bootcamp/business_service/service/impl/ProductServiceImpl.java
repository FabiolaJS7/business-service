package com.bootcamp.business_service.service.impl;

import com.bootcamp.business_service.connector.ProductConnector;
import com.bootcamp.business_service.constants.FamilyTypeProductConstants;
import com.bootcamp.business_service.constants.ProductTypeConstants;
import com.bootcamp.business_service.constants.TypeMovementConstants;
import com.bootcamp.business_service.model.CreateProductRQ;
import com.bootcamp.business_service.model.CreateProductRS;
import com.bootcamp.business_service.service.InfoTransactionManagement;
import com.bootcamp.business_service.service.ProductService;
import com.bootcamp.business_service.util.JsonTransferUtil;
import com.bootcamp.business_service.util.NumberAccountRandom;
import com.bootcamp.commons.bean.products.CustomerBean;
import com.bootcamp.commons.bean.products.InfoTransactionBean;
import com.bootcamp.commons.bean.products.PassiveProductBean;
import com.bootcamp.commons.bean.products.ProductRequest;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.ArrayList;

@Service
@AllArgsConstructor
@Slf4j
public class ProductServiceImpl implements ProductService {


    ProductConnector productConnector;
    InfoTransactionManagement infoTransactionManagement;

    @Override
    public Mono<CreateProductRS> createProduct(Mono<CreateProductRQ> createProductRQ) {
        Mono<ProductRequest> productRequestMono = createProductRQ
                .flatMap(createProductRQ1 -> {
                    //Tengo que crear un ProductRequest
                    ProductRequest productRequest = new ProductRequest();
                    productRequest.setProductType(createProductRQ1.getProductType());
                    CustomerBean customerBean = new CustomerBean();
                    customerBean.setCustomerType("P");
                    customerBean.setCustomerId(createProductRQ1.getCustomerId());
                    productRequest.setCustomer(customerBean);


                    if (createProductRQ1.getFamilyProduct().equalsIgnoreCase(FamilyTypeProductConstants.PASSIVE_FAMILY_TYPE_PRODUCT)) {
                        PassiveProductBean passiveProductBean = new PassiveProductBean();

                        passiveProductBean.setIsFreeCommission(true);
                        passiveProductBean.setAmountOfOpen(0.00);
                        passiveProductBean.setAccountNumber(NumberAccountRandom.generateAccountNumber(createProductRQ1.getProductType()));

                        InfoTransactionBean infoTransactionBean = infoTransactionManagement
                                .buildToPassiveProduct(createProductRQ1.getProductType());
                        passiveProductBean.setInforToTransaction(infoTransactionBean);


                        productRequest.setPassiveProduct(passiveProductBean);
                        productRequest.setHolders(new ArrayList<>());
                        productRequest.setAuthorizedSignatories(new ArrayList<>());

                    }

                    log.info("productRequest: {}", JsonTransferUtil.objectToJson(productRequest));
                    return Mono.just(productRequest);

                });

        return productConnector.createProduct(productRequestMono)
                .flatMap(s -> {
                    CreateProductRS createProductRS = new CreateProductRS();
                    createProductRS.setResult(s);
                    return Mono.just(createProductRS);
                });
    }
}
