package com.bootcamp.business_service.service.impl;

import com.bootcamp.business_service.connector.ProductConnector;
import com.bootcamp.business_service.constants.FamilyTypeProductConstants;
import com.bootcamp.business_service.model.CreateProductRQ;
import com.bootcamp.business_service.model.CreateProductRS;
import com.bootcamp.business_service.service.InfoTransactionManagement;
import com.bootcamp.business_service.service.ProductService;
import com.bootcamp.business_service.util.JsonTransferUtil;
import com.bootcamp.business_service.util.NumberRandomUtil;
import com.bootcamp.commons.bean.products.*;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.LocalDate;
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
                .publishOn(Schedulers.boundedElastic())
                .publishOn(Schedulers.boundedElastic())
                .publishOn(Schedulers.boundedElastic())
                .flatMap(createProductRQ1 -> {
                    //Tengo que crear un ProductRequest
                    ProductRequest productRequest = new ProductRequest();
                    productRequest.setProductType(createProductRQ1.getProductType());
                    CustomerBean customerBean = new CustomerBean();
                    customerBean.setCustomerType("P");
                    customerBean.setCustomerId(createProductRQ1.getCustomerId());
                    productRequest.setCustomer(customerBean);


                    if (createProductRQ1.getFamilyProduct().equalsIgnoreCase(FamilyTypeProductConstants.PASSIVE)) {
                        PassiveProductBean passiveProductBean = new PassiveProductBean();

                        passiveProductBean.setIsFreeCommission(true);
                        passiveProductBean.setAmountOfOpen(0.00);
                        passiveProductBean.setAccountNumber(NumberRandomUtil.generateAccountNumber(createProductRQ1.getProductType()));

                        InfoTransactionBean infoTransactionBean = infoTransactionManagement
                                .buildToPassiveProduct(createProductRQ1.getProductType());
                        passiveProductBean.setInforToTransaction(infoTransactionBean);

                        productRequest.setPassiveProduct(passiveProductBean);
                    } else {
                        ActiveProductBean activeProductBean = new ActiveProductBean();
                        activeProductBean.setHasCreditCard(true);
                        activeProductBean.setCreditLimit(33000.00);
                        activeProductBean.setCreditLimitUsed(0.00);

                        if (Boolean.TRUE.equals(activeProductBean.getHasCreditCard())) {
                            CreditCardBean creditCardBean = new CreditCardBean();
                            creditCardBean.setNumber(NumberRandomUtil.generateNumberCreditCard().block());
                            creditCardBean.setExpirationDate(null);
                            activeProductBean.setCreditCard(creditCardBean);
                        }

                        productRequest.setActiveProduct(activeProductBean);

                    }

                    productRequest.setHolders(new ArrayList<>());
                    productRequest.setAuthorizedSignatories(new ArrayList<>());
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
