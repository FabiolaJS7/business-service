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

@Service
@Slf4j
public class TransactionConnector {

    private final WebClient webClient;

    public TransactionConnector(@Qualifier("webClientTransactionService") WebClient webClient) {
        this.webClient = webClient;
    }

    public Mono<TransactionRS> createTransaction(Mono<TransactionRQ> transactionRQMono) {
        return webClient.post()
                .uri("/api/transactions")
                .body(transactionRQMono, TransactionRQ.class) //Enviado transactionRQ como body
                .retrieve()
                .bodyToMono(TransactionRS.class)
                .doOnError(error -> log.error("Error while create transaction: {}", error.getMessage()));
    }

    public Flux<TransactionRS> getTransactionsByCustomerId(String customerId) {
        return webClient.get()
                .uri("/api/transactions/customers/" + customerId)
                .retrieve()
                .bodyToFlux(TransactionRS.class)
                .doOnError(error -> log.error("Error while getTransactionsByCustomerId: {}", error.getMessage()));
    }

    public Flux<TransactionRS> getTransactionsByProductId(String productId) {
        return webClient.get()
                .uri("/api/transactions/products/" + productId)
                .retrieve()
                .bodyToFlux(TransactionRS.class)
                .doOnError(error -> log.error("Error while getTransactionsByProductId: {}", error.getMessage()));
    }

}
