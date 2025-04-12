package com.bootcamp.business_service.transfer;

import com.bootcamp.business_service.model.CreateProductRQ;
import com.bootcamp.business_service.util.JsonTransferUtil;
import com.bootcamp.commons.bean.products.*;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;


@Component
@Slf4j
@AllArgsConstructor
public class ProductTransfer {

    public Mono<ProductRequest> buildProductRequest(Mono<CreateProductRQ> createProductRQ, String customerType) {
        return createProductRQ
                .doOnNext(productRequest -> log.info("Building product request: " + productRequest))
                .flatMap(createProductRQ1 -> {
                    ProductRequest productRequest = new ProductRequest();
                    productRequest.setProductType(createProductRQ1.getProductType());
                    CustomerBean customerBean = new CustomerBean();
                    customerBean.setCustomerType(customerType);
                    customerBean.setCustomerId(createProductRQ1.getCustomerId());
                    productRequest.setCustomer(customerBean);
                    productRequest.setAmountOfOpen(createProductRQ1.getOpenAmount());
                    log.info("productRequest: {}", JsonTransferUtil.objectToJson(productRequest));
                    return Mono.just(productRequest);

                });
    }
}
