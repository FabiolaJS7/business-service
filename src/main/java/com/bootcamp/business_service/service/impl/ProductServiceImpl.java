package com.bootcamp.business_service.service.impl;

import com.bootcamp.business_service.components.LogicalAddPersonToProduct;
import com.bootcamp.business_service.components.LogicalCreateProduct;
import com.bootcamp.business_service.connector.ProductConnector;

import com.bootcamp.business_service.constants.ActionsConstants;
import com.bootcamp.business_service.constants.ProductTypeConstants;
import com.bootcamp.business_service.model.*;
import com.bootcamp.business_service.service.ProductService;
import com.bootcamp.business_service.transfer.AdditionalPersonTransfer;
import com.bootcamp.business_service.transfer.ProductTransfer;
import com.bootcamp.business_service.util.JsonTransferUtil;
import com.bootcamp.commons.bean.products.ProductRequest;
import com.bootcamp.commons.bean.products.ProductResponse;
import com.bootcamp.commons.bean.products.ProductUpdateRQ;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Optional;


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
                .doOnSubscribe(subscription -> log.info("Create product."))
                .doOnNext(c -> log.info("Create product: {}", c))
                .map(createProductRQ1 -> logicalCreateProduct.validate(Mono.just(createProductRQ1)))
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
                                    })
                                    .doOnNext(productResponse -> log.info("Create product in bd: {}",
                                            JsonTransferUtil.objectToJson(productResponse)));
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
                .doOnSuccess(createProductRS -> log.info("Product creation completed {}",
                        JsonTransferUtil.objectToJson(createProductRS)))
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

    @Override
    public Mono<CreateCardRS> createCardToPassiveProduct(Mono<CreateCardRQ> createCardRQ) {
        return createCardRQ
                .flatMap(rq -> {
                    log.info("Creating Card To PassiveProduct {}", JsonTransferUtil.objectToJson(rq));
                    return productConnector.getProductById(rq.getProductIdToAssociate())
                            .flatMap(productResponse -> {
                                if (Boolean.FALSE.equals(productResponse.getHasPlasticCard() && ProductTypeConstants
                                        .PASSIVE_PRODUCTS.contains(productResponse.getProductType()))){
                                    return updateProductWithPlasticCard(productResponse)
                                            .flatMap(this::getPlasticCardDetails);
                                } else {
                                    CreateCardRS createCardRS = new CreateCardRS();
                                    createCardRS.setCardId("El producto ya cuenta con un plastic card o no es una un producto pasivo");
                                    return Mono.just(createCardRS);
                                }
                            });
                })
                .doOnSuccess(response -> log.info("Created Card To PassiveProduct completed: {}",
                        JsonTransferUtil.objectToJson(response)))
                .doOnError(throwable -> log.info("Error while creating Card To PassiveProduct", throwable));
    }

    // Método para actualizar el producto con la nueva plastic card
    private Mono<ProductResponse> updateProductWithPlasticCard(ProductResponse productResponse) {
        ProductUpdateRQ productUpdateRQ = new ProductUpdateRQ();
        productUpdateRQ.setActionToUpdate(ActionsConstants.PLASTIC_CARD);
        productUpdateRQ.setHasPlasticCard(true);
        return productConnector.updateProduct(productResponse.getId(), Mono.just(productUpdateRQ));
    }

    // Método para setear en el createCardRS los detalles de la tarjeta nueva asociada al producto
    private Mono<CreateCardRS> getPlasticCardDetails(ProductResponse productResponseUpdated) {
        return productConnector.getPlasticCardById(productResponseUpdated.getCardNumber())
                .map(plasticCardBean -> {
                    CreateCardRS createCardRS = new CreateCardRS();
                    createCardRS.setCardId(plasticCardBean.getId());
                    createCardRS.setCardNumber(plasticCardBean.getCardNumber());
                    createCardRS.setTypeCard(plasticCardBean.getCardType());
                    createCardRS.setExpirationDate(plasticCardBean.getExpirationDate());
                    createCardRS.setProductIdAssociated(plasticCardBean.getProductIdAssociated());
                    return createCardRS;
                });
    }


}
