package com.bootcamp.business_service.transfer;

import com.bootcamp.business_service.constants.ProductTypeConstants;
import com.bootcamp.business_service.model.CreateProductRQ;
import com.bootcamp.business_service.components.LogicalTransactionProducts;
import com.bootcamp.business_service.util.JsonTransferUtil;
import com.bootcamp.business_service.util.NumberRandomUtil;
import com.bootcamp.commons.bean.products.*;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.ArrayList;

@Component
@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class ProductTransfer {

    private static final double CREDIT_LIMIT_CC_NEW = 3000.00;

    LogicalTransactionProducts logicalTransactionProducts;

    public Mono<ProductRequest> buildProductRequest(Mono<CreateProductRQ> createProductRQ, String customerType) {
        return createProductRQ
                .flatMap(createProductRQ1 -> {
                    //Tengo que crear un ProductRequest
                    ProductRequest productRequest = new ProductRequest();
                    productRequest.setProductType(createProductRQ1.getProductType());
                    CustomerBean customerBean = new CustomerBean();
                    customerBean.setCustomerType(customerType);
                    customerBean.setCustomerId(createProductRQ1.getCustomerId());
                    productRequest.setCustomer(customerBean);

                    // Valida si el product type pertenece a la familia de PASSIVE PRODUCT de lo contrario es una ACTIVE PRODUCT
                    if (ProductTypeConstants.PASSIVE_PRODUCTS.contains(createProductRQ1.getProductType())) {
                        PassiveProductBean passiveProductBean = new PassiveProductBean();

                        passiveProductBean.setIsFreeCommission(true);
                        passiveProductBean.setAmountOfOpen(createProductRQ1.getOpenAmount());
                        passiveProductBean.setAccountNumber(NumberRandomUtil.generateAccountNumber(createProductRQ1.getProductType()));

                        InfoTransactionBean infoTransactionBean = logicalTransactionProducts
                                .buildToPassiveProduct(createProductRQ1.getProductType(), customerType);
                        passiveProductBean.setInforToTransaction(infoTransactionBean);

                        productRequest.setPassiveProduct(passiveProductBean);
                    } else {
                        ActiveProductBean activeProductBean = new ActiveProductBean();
                        activeProductBean.setHasCreditCard(productRequest.getProductType().equals(ProductTypeConstants.CREDIT_CARD));
                        activeProductBean.setCreditLimit(CREDIT_LIMIT_CC_NEW); //todos los creditos serán 3000
                        activeProductBean.setCreditLimitUsed(Boolean.TRUE.equals(activeProductBean.getHasCreditCard())
                                ? 0 : activeProductBean.getCreditLimit()); //si es tarjeta de credito al ser nueva tiene 0 usado, si es un credito personal o business tiene el monto de la linea de credito
                        activeProductBean.setCreditBalance(Boolean.TRUE.equals(activeProductBean.getHasCreditCard()) ? activeProductBean.getCreditLimit()
                                : 0); //si es CC tiene balance (monto disponible) el monto del credito apertra, si es otro credito tiene balance 0

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
