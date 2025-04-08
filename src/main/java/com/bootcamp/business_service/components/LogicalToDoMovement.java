package com.bootcamp.business_service.components;

import com.bootcamp.business_service.connector.CustomerConnector;
import com.bootcamp.business_service.connector.ProductConnector;
import com.bootcamp.business_service.connector.TransactionConnector;
import com.bootcamp.business_service.constants.MovementTypeConstants;
import com.bootcamp.business_service.constants.ProductTypeConstants;
import com.bootcamp.business_service.model.MovementRQ;
import com.bootcamp.commons.bean.customers.CustomerResponse;
import com.bootcamp.commons.bean.products.BalanceBeanResponse;
import com.bootcamp.commons.bean.products.PassiveProductBean;
import com.bootcamp.commons.bean.products.ProductResponse;
import com.bootcamp.commons.bean.transaction.TransactionRS;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Slf4j
@Component
@AllArgsConstructor
public class LogicalToDoMovement {

    ProductConnector productConnector;
    CustomerConnector customerConnector;
    TransactionConnector transactionConnector;

    public Mono<HashMap<String, String>> validates(Mono<MovementRQ> movementRQ) {

        HashMap<String, String> map = new HashMap<>();
        map.put("enabled", "false");
        map.put("commission", "0");

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

        Flux<TransactionRS> transactions = movementRQ
                .flatMapMany(movementRQ1 -> transactionConnector.getTransactionsByCustomerId(movementRQ1.getCustomerId()))
                .subscribeOn(Schedulers.parallel());


        return Mono.zip(product, customer, balance, productToTransfer.defaultIfEmpty(new ProductResponse()),
                        transactions.collectList().defaultIfEmpty(new ArrayList<>()))
                .flatMap(truple -> {
                    ProductResponse productResponse = truple.getT1();
                    CustomerResponse customerResponse = truple.getT2();
                    BalanceBeanResponse balanceBeanResponse = truple.getT3();
                    ProductResponse productResponseToTransfer = truple.getT4();
                    List<TransactionRS> transactionRS = truple.getT5();

                    return movementRQ.map(movementRQ1 -> {
                        map.put("movementType", movementRQ1.getMovementType());

                        // Validar productos pasivos
                        if (ProductTypeConstants.PASSIVE_PRODUCTS.contains(productResponse.getProductType())) {
                            validationsToPassive(productResponse, transactionRS, movementRQ1, balanceBeanResponse, map);
                        } else {
                            validationsToActive(productResponse, map);
                        }

                        // Agregar información del producto a transferir en caso sea un TRANSFER
                        if (productResponseToTransfer.getId() != null) {
                            map.put("productToTransfer", productResponseToTransfer.getId());
                        }

                        return map;
                    });
                });
    }

    private void validationsToActive(ProductResponse productResponse, HashMap<String, String> map) {
        if (Boolean.TRUE.equals(productResponse.getActiveProduct().getHasCreditCard())) {
            map.put("enabled", "true");
        }

    }

    private void validationsToPassive(ProductResponse productResponse, List<TransactionRS> transactions,
                                      MovementRQ movementRQ, BalanceBeanResponse balanceBeanResponse,
                                      HashMap<String, String> map) {

        PassiveProductBean passiveProduct = productResponse.getPassiveProduct();

        // Validación para el límite máximo de movimientos por mes para asignar comisión si se pasa el límite
        if (Integer.valueOf(passiveProduct.getInforToTransaction().getMaxPerMonth()) >= transactions.size()) {
            map.put("commission", String.valueOf(passiveProduct.getInforToTransaction().getCommission()));
        }

        // Si es deposito que lo deje pasar
        if (MovementTypeConstants.DEPOSIT.equalsIgnoreCase(movementRQ.getMovementType())) {
            map.put("enabled", "true");
        } else {
            // si es retiro el balance de la cuenta tiene que ser mayor al monto por retirar
            if (balanceBeanResponse.getBalanceAmount() > movementRQ.getAmount()) {
                map.put("enabled", "true");
            }
        }

    }
}
