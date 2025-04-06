package com.bootcamp.business_service.service.impl;

import com.bootcamp.business_service.components.LogicalAddPersonToProduct;
import com.bootcamp.business_service.components.LogicalCreateProduct;
import com.bootcamp.business_service.connector.ProductConnector;

import com.bootcamp.business_service.model.AdditionalPersonRQ;
import com.bootcamp.business_service.model.AdditionalPersonRS;
import com.bootcamp.business_service.model.CreateProductRQ;
import com.bootcamp.business_service.model.CreateProductRS;
import com.bootcamp.business_service.service.ProductService;
import com.bootcamp.business_service.transfer.AdditionalPersonTransfer;
import com.bootcamp.business_service.transfer.ProductTransfer;
import com.bootcamp.business_service.util.JsonTransferUtil;
import com.bootcamp.commons.bean.products.ProductRequest;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
@AllArgsConstructor
@Slf4j
public class ProductServiceImpl implements ProductService {


    ProductConnector productConnector;
    LogicalCreateProduct logicalCreateProduct;
    LogicalAddPersonToProduct logicalAddPersonToProduct;
    ProductTransfer productTransfer;
    AdditionalPersonTransfer additionalPersonTransfer;

    @Override
    public Mono<CreateProductRS> createProduct(Mono<CreateProductRQ> createProductRQ) {

        return createProductRQ
                .map(createProductRQ1 -> logicalCreateProduct.validate(Mono.just(createProductRQ1)))
                .doOnSubscribe(subscription -> log.info("Product creation started"))
                .flatMap(hashMapMono ->
                    hashMapMono.flatMap(attributesMainMap -> {
                        if (attributesMainMap.get("enabled").equalsIgnoreCase("true")) {
                            // Construir el ProductRequest y llamar al conector
                            Mono<ProductRequest> requestMono = productTransfer.buildProductRequest(createProductRQ,
                                    attributesMainMap.get("customerType"));
                            return productConnector.createProduct(requestMono)
                                    .flatMap(productId -> {
                                        // Crear la respuesta con el productId
                                        CreateProductRS createProductRS = new CreateProductRS();
                                        createProductRS.setProductId(productId); // Asignar el valor del Mono<String>
                                        createProductRS.setResult(true);
                                        return Mono.just(createProductRS);
                                    });
                        } else {
                            // Crear la respuesta con el mensaje de error
                            CreateProductRS createProductRS = new CreateProductRS();
                            createProductRS.setProductId(null);
                            createProductRS.setResult(false);
                            createProductRS.setMessage(attributesMainMap.get("message"));
                            return Mono.just(createProductRS);
                        }
                    })

                )
                .doOnSuccess(createProductRS -> log.info("Product creation completed {}"
                        , JsonTransferUtil.objectToJson(createProductRS)))
                .doOnError(throwable -> log.error("Product creation failed", throwable));

    }

    @Override
    public Mono<AdditionalPersonRS> updateAdditionalPerson(Mono<AdditionalPersonRQ> additionalPersonRQ) {
        return additionalPersonRQ
                .flatMap(a -> logicalAddPersonToProduct.validate(additionalPersonRQ)
                        .flatMap(isValid -> {
                            if (Boolean.TRUE.equals(isValid)) {
                                return additionalPersonTransfer.buildProductUpdateRQ(additionalPersonRQ)
                                        .flatMap(productUpdateRQ -> productConnector.updateProduct(a.getProductId(),
                                                Mono.just(productUpdateRQ)))
                                        .map(productResponse -> {
                                            AdditionalPersonRS additionalPersonRS = new AdditionalPersonRS();
                                            additionalPersonRS.setResult(true);
                                            return additionalPersonRS;
                                        });
                            } else {
                                AdditionalPersonRS additionalPersonRS = new AdditionalPersonRS();
                                additionalPersonRS.setResult(false);
                                return Mono.just(additionalPersonRS);
                            }
                        })
                ).doOnSubscribe(subscription -> log.info("Updating addition person"))
                .doOnSuccess(response -> log.info("Update completed: {}", JsonTransferUtil.objectToJson(response)))
                .doOnError(throwable -> log.info("Error updating additional person", throwable));
    }


}
