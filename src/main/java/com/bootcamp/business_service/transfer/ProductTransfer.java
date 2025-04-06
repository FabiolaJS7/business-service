package com.bootcamp.business_service.transfer;

import com.bootcamp.business_service.constants.FamilyTypeProductConstants;
import com.bootcamp.business_service.constants.ProductTypeConstants;
import com.bootcamp.business_service.model.CreateProductRQ;
import com.bootcamp.business_service.service.InfoTransactionManagement;
import com.bootcamp.business_service.util.JsonTransferUtil;
import com.bootcamp.business_service.util.NumberRandomUtil;
import com.bootcamp.commons.bean.products.*;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.ArrayList;

@Component
@Slf4j
@AllArgsConstructor
public class ProductTransfer {

    InfoTransactionManagement infoTransactionManagement;

    public Mono<ProductRequest> buildProductRequest(Mono<CreateProductRQ> createProductRQ) {
        return createProductRQ
                .flatMap(createProductRQ1 -> {
                    //Tengo que crear un ProductRequest
                    ProductRequest productRequest = new ProductRequest();
                    productRequest.setProductType(createProductRQ1.getProductType());
                    CustomerBean customerBean = new CustomerBean();
                    customerBean.setCustomerType("P");
                    customerBean.setCustomerId(createProductRQ1.getCustomerId());
                    productRequest.setCustomer(customerBean);

                    // Valida si el product type pertenece a la familia de PASSIVE PRODUCT de lo contrario es una ACTIVE PRODUCT
                    if (ProductTypeConstants.PASSIVE_PRODUCTS.contains(createProductRQ1.getProductType())) {
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
                        activeProductBean.setHasCreditCard(productRequest.getProductType().equals(ProductTypeConstants.CREDIT_CARD));
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
    }
}
