package com.bootcamp.business_service.service.impl;

import com.bootcamp.business_service.connector.ProductConnector;

import com.bootcamp.business_service.model.CreateProductRQ;
import com.bootcamp.business_service.model.CreateProductRS;
import com.bootcamp.business_service.service.EnabledToCreateProduct;
import com.bootcamp.business_service.service.ProductService;
import com.bootcamp.business_service.transfer.ProductTransfer;
import com.bootcamp.business_service.util.JsonTransferUtil;
import com.bootcamp.commons.bean.products.ProductRequest;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@AllArgsConstructor
@Slf4j
public class ProductServiceImpl implements ProductService {


    ProductConnector productConnector;
    EnabledToCreateProduct enabledToCreateProduct;
    ProductTransfer productTransfer;

    @Override
    public Mono<CreateProductRS> createProduct(Mono<CreateProductRQ> createProductRQ) {

        return createProductRQ
                .map(createProductRQ1 -> enabledToCreateProduct.validate(Mono.just(createProductRQ1)))
                .doOnSubscribe(subscription -> log.info("Product creation started"))
                .flatMap(hashMapMono ->
                    hashMapMono.flatMap(stringStringHashMap -> {
                        if (stringStringHashMap.get("enabled").equalsIgnoreCase("true")) {
                            // Construir el ProductRequest y llamar al conector
                            Mono<ProductRequest> requestMono = productTransfer.buildProductRequest(createProductRQ);
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
                            createProductRS.setMessage(stringStringHashMap.get("message"));
                            return Mono.just(createProductRS);
                        }
                    })

                )
                .doOnSuccess(createProductRS -> log.info("Product creation completed {}"
                        , JsonTransferUtil.objectToJson(createProductRS)))
                .doOnError(throwable -> log.error("Product creation failed", throwable));

    }


}
