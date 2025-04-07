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

        return Mono.zip(product, customer, balance)
                .flatMap(truple -> {
                    ProductResponse productResponse = truple.getT1();
                    CustomerResponse customerResponse = truple.getT2();
                    BalanceBeanResponse balanceBeanResponse = truple.getT3();

                    return movementRQ
                            .map(movementRQ1 -> {
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
}
