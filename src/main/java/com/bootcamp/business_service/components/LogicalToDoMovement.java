package com.bootcamp.business_service.components;

import com.bootcamp.business_service.connector.CustomerConnector;
import com.bootcamp.business_service.connector.ProductConnector;
import com.bootcamp.business_service.connector.ProductTypeConnector;
import com.bootcamp.business_service.connector.TransactionConnector;
import com.bootcamp.business_service.constants.MovementTypeConstants;
import com.bootcamp.business_service.constants.ProductTypeConstants;
import com.bootcamp.business_service.model.MovementRQ;
import com.bootcamp.commons.bean.customers.CustomerResponse;
import com.bootcamp.commons.bean.products.BalanceBeanResponse;
import com.bootcamp.commons.bean.products.ProductResponse;
import com.bootcamp.commons.bean.products.ProductTypeResponse;
import com.bootcamp.commons.bean.transaction.TransactionRS;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.HashMap;

@Slf4j
@Component
@AllArgsConstructor
public class LogicalToDoMovement {

    private static final String ENABLED = "enabled";
    private static final String COMMISSION = "commission";
    private static final String MESSAGE = "message";

    ProductConnector productConnector;
    CustomerConnector customerConnector;
    TransactionConnector transactionConnector;
    ProductTypeConnector productTypeConnector;

    public Mono<HashMap<String, String>> validates(Mono<MovementRQ> movementRQ) {

        HashMap<String, String> map = new HashMap<>();
        map.put(ENABLED, Boolean.FALSE.toString());
        map.put(COMMISSION, "0");
        map.put(MESSAGE, "*");

        return movementRQ
                .flatMap(movement -> {

                    map.put("movementType", movement.getMovementType());

                    // Obtiene el registro del producto
                    Mono<ProductResponse> product = productConnector.getProductById(movement.getProductId())
                            .subscribeOn(Schedulers.parallel());
                    // Obtiene información del cliente
                    Mono<CustomerResponse> customer = customerConnector.getCustomerById(movement.getCustomerId())
                            .subscribeOn(Schedulers.parallel());
                    // Obtiene el balance del producto
                    Mono<BalanceBeanResponse> balance = productConnector.findBalanceByProductId(movement.getProductId())
                            .subscribeOn(Schedulers.parallel());

                    Mono<ProductResponse> productToTransfer = Mono.defer(() -> {
                        if (movement.getAccountNumberToTransfer() != null && !movement.getAccountNumberToTransfer().equalsIgnoreCase("")) {
                            return productConnector.getProductByAccountNumber(movement.getAccountNumberToTransfer())
                                    .subscribeOn(Schedulers.parallel());
                        } else {
                            return Mono.just(new ProductResponse());
                        }
                    });

                    return Mono.zip(product, customer, balance, productToTransfer.defaultIfEmpty(new ProductResponse()))
                            .flatMap(truple -> {
                                ProductResponse productResponse = truple.getT1();
                                CustomerResponse customerResponse = truple.getT2();
                                BalanceBeanResponse balanceBeanResponse = truple.getT3();
                                ProductResponse productToTransferResponse = truple.getT4();

                                // Obtiene la lista de transactiones realizadas en el mes para el producto
                                Flux<TransactionRS> transactions = movementRQ
                                        .flatMapMany(movementRQ1 -> transactionConnector.getTransactionsByProductId(productResponse.getId(), null, null));
                                Mono<ProductTypeResponse> productType = productTypeConnector.getProductTypeByCode(productResponse.getProductType());

                                if (movement.getMovementType().equals(MovementTypeConstants.TRANSFER)) {
                                    map.put("productToTransfer", productToTransferResponse.getId());
                                }

                                return transactions.count()
                                        .map(Long::intValue)
                                        .flatMap(numberOfTransctions ->
                                                productType
                                                        .map(productTypeResponse -> {
                                                            if (ProductTypeConstants.PASSIVE_PRODUCTS.contains(productResponse.getProductType())) {
                                                                // Validación si pasa el límite máximo de movimientos por mes se asigna comisión
                                                                if (numberOfTransctions >= productTypeResponse.getMaxMovementPerMonth().intValue()) {
                                                                    map.put(COMMISSION, String.valueOf(productTypeResponse.getMovementCommission()));
                                                                } else {
                                                                    map.put(COMMISSION, "0.00");
                                                                }

                                                                if (MovementTypeConstants.DEPOSIT.equalsIgnoreCase(movement.getMovementType())) {
                                                                    map.put(ENABLED, "true");
                                                                } else {
                                                                    // Si es retiro, validamos que el balance sea mayor al monto a retirar
                                                                    if (balanceBeanResponse.getTotalAmountInAccount() >= movement.getAmount()) {
                                                                        map.put(ENABLED, "true");
                                                                    } else {
                                                                        map.put(ENABLED, "false");
                                                                        map.put(MESSAGE, "No hay monto suficiente para retiro");
                                                                    }
                                                                }
                                                            } else {
                                                                if (MovementTypeConstants.CONSUME.equalsIgnoreCase(movement.getMovementType())) {
                                                                    if (ProductTypeConstants.CREDIT_CARD.equalsIgnoreCase(productResponse.getProductType())
                                                                            && movement.getAmount() <= balanceBeanResponse.getCreditEnabledToUse()) {
                                                                        map.put(ENABLED, "true");
                                                                    } else {
                                                                        map.put(MESSAGE, "No cuenta con fondos en la tarjeta de credito (CC)");
                                                                    }
                                                                } else if (MovementTypeConstants.PAYMENT.equalsIgnoreCase(movement.getMovementType())) {
                                                                    map.put(ENABLED, "true");
                                                                } else  {
                                                                    map.put(MESSAGE, "Cuentas de crédito no aplica para movimiento " + movement.getMovementType());
                                                                }


                                                            }


                                                            return map;
                                                        })
                                        );

                            });
                })
                .onErrorResume(throwable -> {
                    map.put(ENABLED, "false");
                    map.put(MESSAGE, "Error occurred while consulting some service.");
                    return Mono.just(map);
                })
                .doOnSuccess(stringStringHashMap -> log.info("validates: {}", stringStringHashMap));
    }
}
