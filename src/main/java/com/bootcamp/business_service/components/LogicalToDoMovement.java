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

    ProductConnector productConnector;
    CustomerConnector customerConnector;
    TransactionConnector transactionConnector;
    ProductTypeConnector productTypeConnector;

    public Mono<HashMap<String, String>> validates(Mono<MovementRQ> movementRQ) {

        return movementRQ
                .flatMap(movement -> {
                    HashMap<String, String> map = new HashMap<>();
                    map.put("enabled", "false");
                    map.put("commission", "0");
                    map.put("message", "*");

                    // Obtiene el registro del producto
                    Mono<ProductResponse> product = productConnector.getProductById(movement.getProductId())
                            .subscribeOn(Schedulers.parallel());
                    // Obtiene información del cliente
                    Mono<CustomerResponse> customer =customerConnector.getCustomerById(movement.getCustomerId())
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

                                // Obtiene la lista de transactiones realizadas para el producto
                                Flux<TransactionRS> transactions = movementRQ
                                        .flatMapMany(movementRQ1 -> transactionConnector.getTransactionsByProductId(productResponse.getId()));
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
                                                                    map.put("commission", String.valueOf(productTypeResponse.getMovementCommission()));
                                                                } else {
                                                                    map.put("commission", "0.00");
                                                                }

                                                                if (MovementTypeConstants.DEPOSIT.equalsIgnoreCase(movement.getMovementType())) {
                                                                    map.put("enabled", "true");
                                                                } else {
                                                                    // Si es retiro, validamos que el balance sea mayor al monto a retirar
                                                                    if (balanceBeanResponse.getTotalAmountInAccount() >= movement.getAmount()) {
                                                                        map.put("enabled", "true");
                                                                    } else {
                                                                        map.put("enabled", "false");
                                                                        map.put("message", "No hay monto suficiente para retiro");
                                                                    }
                                                                }
                                                            } else {
                                                                if (MovementTypeConstants.CONSUME.equalsIgnoreCase(movement.getMovementType())) {
                                                                    if (ProductTypeConstants.CREDIT_CARD.equalsIgnoreCase(productResponse.getProductType())
                                                                            && movement.getAmount() <= balanceBeanResponse.getCreditEnabledToUse()) {
                                                                        map.put("enabled", "true");
                                                                    } else {
                                                                        map.put("message", "No cuenta con fondos en la tarjeta de credito (CC)");
                                                                    }
                                                                } else if (MovementTypeConstants.PAYMENT.equalsIgnoreCase(movement.getMovementType())) {
                                                                    map.put("enabled", "true");
                                                                } else  {
                                                                    map.put("message", "Cuentas de crédito no aplica para movimiento " + movement.getMovementType());
                                                                }


                                                            }


                                                            return map;
                                                        })
                                        );

                            });
                })
                .doOnSuccess(stringStringHashMap -> log.info("validates: {}", stringStringHashMap));
    }
}
