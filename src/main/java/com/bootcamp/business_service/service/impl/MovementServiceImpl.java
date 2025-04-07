package com.bootcamp.business_service.service.impl;

import com.bootcamp.business_service.components.LogicalToDoMovement;
import com.bootcamp.business_service.connector.CustomerConnector;
import com.bootcamp.business_service.connector.ProductConnector;
import com.bootcamp.business_service.connector.TransactionConnector;
import com.bootcamp.business_service.constants.MovementTypeConstants;
import com.bootcamp.business_service.model.MovementRQ;
import com.bootcamp.business_service.model.MovementRS;
import com.bootcamp.business_service.service.MovementService;
import com.bootcamp.commons.bean.products.BalanceBeanRequest;
import com.bootcamp.commons.bean.products.BalanceBeanResponse;
import com.bootcamp.commons.bean.transaction.TransactionRQ;
import com.bootcamp.commons.bean.transaction.TransactionRS;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@AllArgsConstructor
@Slf4j
public class MovementServiceImpl implements MovementService {

    ProductConnector productConnector;
    CustomerConnector customerConnector;
    LogicalToDoMovement logicalToDoMovement;
    TransactionConnector transactionConnector;

    @Override
    public Mono<MovementRS> doMovementToTransaction(Mono<MovementRQ> movementRQMono) {
        return movementRQMono
                .flatMap(movementRQ ->
                        logicalToDoMovement.validate(movementRQMono)
                                .flatMap(aBoolean -> {
                                    if (Boolean.TRUE.equals(aBoolean)) {
                                        BalanceBeanRequest balanceBeanRequest = new BalanceBeanRequest();
                                        balanceBeanRequest.setMovementType(movementRQ.getMovementType());
                                        balanceBeanRequest.setAmount(movementRQ.getAmount());
                                        return  productConnector.updateBalance(movementRQ.getProductId(), Mono.just(balanceBeanRequest));

                                    } else {
                                        BalanceBeanResponse emptyResponse = new BalanceBeanResponse();
                                        emptyResponse.setResult("false");
                                        return Mono.just(emptyResponse);
                                    }
                                })
                                .flatMap(balanceBeanResponse -> {
                                    // Crear TransactionRQ y realizar la transacción
                                    TransactionRQ transactionRQ = new TransactionRQ();
                                    transactionRQ.setProductId(movementRQ.getProductId());
                                    transactionRQ.setAmount(movementRQ.getAmount());
                                    transactionRQ.setMovementType(movementRQ.getMovementType());
                                    transactionRQ.setResult(balanceBeanResponse.getResult());

                                    return transactionConnector.createTransaction(Mono.just(transactionRQ))
                                            .map(transactionRS -> {
                                                MovementRS movementRS = new MovementRS();
                                                movementRS.setResult(transactionRS.getResult());
                                                movementRS.setMessage(transactionRS.getObservation());
                                                return movementRS;
                                            });

                        })
                )
                .doOnSuccess(movementRS -> log.info("Transaction completed successfully: {}", movementRS))
                .doOnError(error -> log.error("Error during transaction: {}", error.getMessage()));


    }
}
