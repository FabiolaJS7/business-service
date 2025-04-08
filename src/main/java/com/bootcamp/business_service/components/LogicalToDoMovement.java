package com.bootcamp.business_service.components;

import com.bootcamp.business_service.connector.CustomerConnector;
import com.bootcamp.business_service.connector.ProductConnector;
import com.bootcamp.business_service.constants.MovementTypeConstants;
import com.bootcamp.business_service.constants.ProductTypeConstants;
import com.bootcamp.business_service.model.MovementRQ;
import com.bootcamp.commons.bean.customers.CustomerResponse;
import com.bootcamp.commons.bean.products.BalanceBeanResponse;
import com.bootcamp.commons.bean.products.ProductResponse;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.HashMap;

@Slf4j
@Component
@AllArgsConstructor
public class LogicalToDoMovement {

    ProductConnector productConnector;
    CustomerConnector customerConnector;

    public Mono<Boolean> validate(Mono<MovementRQ> movementRQ) {
        Mono<ProductResponse> product = movementRQ
                .flatMap(movementRQ1 -> productConnector.getProductById(movementRQ1.getProductId()))
                .subscribeOn(Schedulers.parallel());

        Mono<CustomerResponse> customer = movementRQ
                .flatMap(movementRQ1 -> customerConnector.getCustomerById(movementRQ1.getCustomerId()))
                .subscribeOn(Schedulers.parallel());

        Mono<BalanceBeanResponse> balance = movementRQ
                .flatMap(movementRQ1 -> productConnector.findBalanceByProductId(movementRQ1.getProductId()))
                .subscribeOn(Schedulers.parallel());

        Mono<ProductResponse> secondProductTotransfer = movementRQ
                .flatMap(m -> {
                    if (m.getMovementType().equals(MovementTypeConstants.TRANSFER)) {
                        return productConnector.getProductByAccountNumber(m.getAccountNumberToTransfer());
                    }
                    return null;
                }).subscribeOn(Schedulers.parallel());


        return Mono.zip(product, customer, balance, secondProductTotransfer)
                .flatMap(truple -> {
                    ProductResponse productResponse = truple.getT1();
                    CustomerResponse customerResponse = truple.getT2();
                    BalanceBeanResponse balanceBeanResponse = truple.getT3();
                    ProductResponse secondProductResponse = truple.getT4();

                    return movementRQ
                            .map(movementRQ1 -> { //PASSIVE libre comision por mantenimiento y limite máximo de movimientos mensuales
                                if (ProductTypeConstants.PASSIVE_PRODUCTS.contains(productResponse.getProductType())) {
                                    if (movementRQ1.getMovementType().equalsIgnoreCase(MovementTypeConstants.DEPOSIT)) {
                                        return true;
                                    } else {
                                        return balanceBeanResponse.getBalanceAmount() > movementRQ1.getAmount();
                                    }
                                } else {
                                    return true;
                                }

                            }).defaultIfEmpty(false);
                });
    }

    public Mono<HashMap<String, String>> validates(Mono<MovementRQ> movementRQ) {

        HashMap<String, String> map = new HashMap<>();
        map.put("enabled", "false");
        map.put("commission","0");

        Mono<ProductResponse> product = movementRQ
                .flatMap(movementRQ1 -> productConnector.getProductById(movementRQ1.getProductId()))
                .subscribeOn(Schedulers.parallel());

        Mono<CustomerResponse> customer = movementRQ
                .flatMap(movementRQ1 -> customerConnector.getCustomerById(movementRQ1.getCustomerId()))
                .subscribeOn(Schedulers.parallel());

        Mono<BalanceBeanResponse> balance = movementRQ
                .flatMap(movementRQ1 -> productConnector.findBalanceByProductId(movementRQ1.getProductId()))
                .subscribeOn(Schedulers.parallel());

        Mono<ProductResponse> productToTransfer = movementRQ
                .filter(m -> MovementTypeConstants.TRANSFER.equals(m.getMovementType()))
                .flatMap(m -> productConnector.getProductByAccountNumber(m.getAccountNumberToTransfer()))
                .subscribeOn(Schedulers.parallel());


        return Mono.zip(product, customer, balance, productToTransfer.defaultIfEmpty(new ProductResponse()))
                .flatMap(truple -> {
                    ProductResponse productResponse = truple.getT1();
                    CustomerResponse customerResponse = truple.getT2();
                    BalanceBeanResponse balanceBeanResponse = truple.getT3();
                    ProductResponse productResponseToTransfer = truple.getT4();

                    return movementRQ.map(movementRQ1 -> {
                        map.put("movementType", movementRQ1.getMovementType());
                        map.put("movementType", movementRQ1.getMovementType());

                        // Validar productos pasivos
                        if (ProductTypeConstants.PASSIVE_PRODUCTS.contains(productResponse.getProductType())) {
                            if (Boolean.FALSE.equals(productResponse.getPassiveProduct().getIsFreeCommission())) {
                                map.put("commission", String.valueOf(productResponse.getPassiveProduct()
                                        .getInforToTransaction().getCommission()));
                            }
                            if (MovementTypeConstants.DEPOSIT.equalsIgnoreCase(movementRQ1.getMovementType())) {
                                map.put("enabled", "true");
                            } else  {
                                if (balanceBeanResponse.getBalanceAmount() > movementRQ1.getAmount()) {
                                    map.put("enabled", "true");
                                }
                            }
                        } else {
                            if (Boolean.TRUE.equals(productResponse.getActiveProduct().getHasCreditCard())) {
                                map.put("enabled", "true");
                            }

                        }

                        // Agregar información del producto a transferir si aplica
                        if (productResponseToTransfer.getId() != null) {
                            map.put("productToTransfer", productResponseToTransfer.getId());
                        }

                        return map;
                    });
                });


    }
}
