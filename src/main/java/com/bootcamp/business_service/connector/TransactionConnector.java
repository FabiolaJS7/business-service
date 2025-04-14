package com.bootcamp.business_service.connector;

import com.bootcamp.business_service.util.JsonTransferUtil;
import com.bootcamp.commons.bean.transaction.TransactionRQ;
import com.bootcamp.commons.bean.transaction.TransactionRS;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.util.Optional;

@Service
@Slf4j
public class TransactionConnector {

    private final WebClient webClient;

    public TransactionConnector(@Qualifier("webClientTransactionService") WebClient webClient) {
        this.webClient = webClient;
    }

    public Mono<TransactionRS> createTransaction(Mono<TransactionRQ> transactionRQMono) {
        return transactionRQMono
                .doOnNext(rq ->  log.info("API Create Transaction RQ{}",
                        JsonTransferUtil.objectToJson(transactionRQMono)))
                .flatMap(transactionRQ -> webClient.post()
                        .uri("/api/transactions")
                        .bodyValue(transactionRQ) //Enviado transactionRQ como body
                        .retrieve()
                        .bodyToMono(TransactionRS.class)
                        .doOnNext(rs -> log.info("API Create Transaction RS{}", JsonTransferUtil.objectToJson(rs)))
                        .doOnError(error -> log.error("Error while create transaction: {}", error.getMessage()))

                );
    }

    public Flux<TransactionRS> getTransactionsByCustomerId(String customerId) {
        return webClient.get()
                .uri("/api/transactions/customers/" + customerId)
                .retrieve()
                .bodyToFlux(TransactionRS.class)
                .doOnError(error -> log.error("Error while getTransactionsByCustomerId: {}", error.getMessage()));
    }

    public Flux<TransactionRS> getTransactionsByProductId(String productId, LocalDate startDate, LocalDate endDate) {
        log.info("API getTransactionsByProductId {} and dates from {}, to {}", productId, startDate, endDate);
        return webClient.get()
                .uri(uriBuilder -> {
                    // Construye dinámicamente la URI con parámetros opcionales
                    uriBuilder.path("/api/transactions/products/{productId}")
                            .queryParamIfPresent("startDate", Optional.ofNullable(startDate))
                            .queryParamIfPresent("endDate", Optional.ofNullable(endDate));
                    return uriBuilder.build(productId);
                })
                .retrieve()
                .bodyToFlux(TransactionRS.class)
                .doOnError(error -> log.error("Error while getTransactionsByProductId: {}", error.getMessage()));
    }

}
