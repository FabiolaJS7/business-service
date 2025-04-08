package com.bootcamp.business_service.service.impl;

import com.bootcamp.business_service.components.LogicalToDoMovement;
import com.bootcamp.business_service.connector.ProductConnector;
import com.bootcamp.business_service.connector.TransactionConnector;
import com.bootcamp.business_service.constants.MovementTypeConstants;
import com.bootcamp.business_service.model.MovementRQ;
import com.bootcamp.business_service.model.MovementRS;
import com.bootcamp.business_service.service.MovementService;
import com.bootcamp.business_service.util.JsonTransferUtil;
import com.bootcamp.commons.bean.products.BalanceBeanRequest;
import com.bootcamp.commons.bean.products.BalanceBeanResponse;
import com.bootcamp.commons.bean.transaction.TransactionRQ;
import com.bootcamp.commons.bean.transaction.TransactionRS;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.HashMap;

@Service
@AllArgsConstructor
@Slf4j
public class MovementServiceImpl implements MovementService {

    ProductConnector productConnector;
    LogicalToDoMovement logicalToDoMovement;
    TransactionConnector transactionConnector;

    @Override
    public Mono<MovementRS> doMovementToTransaction(Mono<MovementRQ> movementRQMono) {
        return movementRQMono
                .flatMap(movementRQ ->
                        logicalToDoMovement.validates(movementRQMono)
                                .flatMap(map -> this.updateMovementInBalance(map, movementRQ)
                                        .flatMap(balanceBeanResponse -> {
                                            return this.saveTransaction(movementRQ, balanceBeanResponse, map)
                                                    .map(transactionRS -> {
                                                        MovementRS movementRS = new MovementRS();
                                                        movementRS.setResult(transactionRS.getResult());
                                                        movementRS.setMessage(transactionRS.getObservation());
                                                        return movementRS;
                                                    });
                                        })
                                )
                )
                .doOnSuccess(movementRS -> log.info("Transaction completed successfully: {}", movementRS))
                .doOnError(error -> log.error("Error during transaction: {}", error.getMessage()));


    }

    private Mono<TransactionRS> saveTransaction(MovementRQ movementRQ, BalanceBeanResponse balanceBeanResponse,
                                                HashMap<String, String> map) {
        // Crear TransactionRQ y realizar la transacción
        TransactionRQ transactionRQ = new TransactionRQ();
        transactionRQ.setProductId(movementRQ.getProductId());
        transactionRQ.setCustomerId(movementRQ.getCustomerId());
        transactionRQ.setAmountMoved(movementRQ.getAmount());
        transactionRQ.setMovementType(movementRQ.getMovementType());
        transactionRQ.setResult(balanceBeanResponse.getResult());
        transactionRQ.setCommissionAmount(Double.parseDouble(map.get("commission")));
        transactionRQ.setAmount(transactionRQ.getAmountMoved() - transactionRQ.getCommissionAmount());
        return transactionConnector.createTransaction(Mono.just(transactionRQ));
    }

    private Mono<BalanceBeanResponse> updateMovementInBalance(HashMap<String, String> map, MovementRQ movementRQ) {
        if (!map.get("enabled").equalsIgnoreCase("true")) {
            // Si no está habilitado, devolver una respuesta false
            BalanceBeanResponse emptyResponse = new BalanceBeanResponse();
            emptyResponse.setResult("false");
            return Mono.just(emptyResponse);
        }

        // Crear la solicitud de balance para la cuenta principal
        BalanceBeanRequest balanceBeanRequest = new BalanceBeanRequest();
        balanceBeanRequest.setMovementType(
                MovementTypeConstants.TRANSFER.equalsIgnoreCase(movementRQ.getMovementType())
                        ? MovementTypeConstants.WITHDRAW
                        : movementRQ.getMovementType()
        );
        balanceBeanRequest.setAmount(movementRQ.getAmount());

        // Actualizar el balance de la cuenta principal
        return productConnector.updateBalance(movementRQ.getProductId(), Mono.just(balanceBeanRequest))
                .doOnSubscribe(s -> log.info("Sending update balance: {}",
                        JsonTransferUtil.objectToJson(balanceBeanRequest)))
                .doOnSuccess(balanceBeanResponse -> log.info("Update balance response: {}",
                        JsonTransferUtil.objectToJson(balanceBeanResponse)))
                .doOnError(error -> log.error("Error while updating balance: {}", error.getMessage()))
                .flatMap(balanceBeanResponse -> {
                    // Si es una transferencia, actualizar la cuenta de destino
                    if (MovementTypeConstants.TRANSFER.equalsIgnoreCase(map.get("movementType"))) {
                        BalanceBeanRequest secondBalanceRequest = new BalanceBeanRequest();
                        secondBalanceRequest.setMovementType(MovementTypeConstants.DEPOSIT);
                        secondBalanceRequest.setAmount(movementRQ.getAmount());

                        return productConnector.updateBalance(map.get("productToTransfer"), Mono.just(secondBalanceRequest))
                                .doOnSubscribe(s -> log.info("Sending update balance for second account: {}",
                                        JsonTransferUtil.objectToJson(secondBalanceRequest)))
                                .doOnSuccess(secondBalanceResponse -> log.info("Update balance response for second account: {}"
                                        , JsonTransferUtil.objectToJson(secondBalanceResponse)))
                                .doOnError(error -> log.error("Error while updating balance for second account: {}"
                                        , error.getMessage()))
                                .thenReturn(balanceBeanResponse); // Retornar la respuesta original después de actualizar la segunda cuenta
                    }
                    return Mono.just(balanceBeanResponse); // Si no es transferencia, devolver la respuesta original
                });
    }
}
