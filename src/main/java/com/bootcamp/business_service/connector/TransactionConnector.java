package com.bootcamp.business_service.connector;

import com.bootcamp.business_service.util.JsonTransferUtil;
import com.bootcamp.commons.bean.transaction.TransactionRQ;
import com.bootcamp.commons.bean.transaction.TransactionRS;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
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
    AuthConnector authConnector;

    public TransactionConnector(@Qualifier("webClientService") WebClient webClient, AuthConnector authConnector) {
        this.webClient = webClient;
        this.authConnector = authConnector;
    }

    // Endpoint de transaction API para crear una transacción
    @CircuitBreaker(name = "transactionService", fallbackMethod = "fallCreateTransaction")
    public Mono<TransactionRS> createTransaction(Mono<TransactionRQ> transactionRQMono) {
        return authConnector.getAuthToken()
                .flatMap(token ->  transactionRQMono
                        .doOnNext(rq ->  log.info("API Create Transaction RQ{}",
                                JsonTransferUtil.objectToJson(transactionRQMono)))
                        .flatMap(transactionRQ -> webClient.post()
                                .uri("/api/transactions")
                                .header("Authorization", token)
                                .bodyValue(transactionRQ) //Enviado transactionRQ como body
                                .retrieve()
                                .bodyToMono(TransactionRS.class)
                                .doOnNext(rs -> log.info("API Create Transaction RS{}", JsonTransferUtil.objectToJson(rs)))
                                .doOnError(error -> log.error("Error while create transaction: {}", error.getMessage()))
                        )
                );
    }

    public Flux<TransactionRS> getTransactionsByCustomerId(String customerId) {
        return webClient.get()
                .uri("/api/transactions/customers/" + customerId)
                .retrieve()
                .bodyToFlux(TransactionRS.class)
                .doOnError(error -> log.error("Error while getTransactionsByCustomerId: {}", error.getMessage()));
    }

    // Endpoint de transaction API para obtener las transacciones por productId
    @CircuitBreaker(name = "transactionService", fallbackMethod = "fallBackGetTransactionsByProductId")
    public Flux<TransactionRS> getTransactionsByProductId(String productId, LocalDate startDate, LocalDate endDate) {
        log.info("API getTransactionsByProductId {} and dates from {}, to {}", productId, startDate, endDate);
        return authConnector.getAuthToken()
                .flatMapMany(token -> webClient.get()
                        .uri(uriBuilder -> {
                            // Construye dinámicamente la URI con parámetros opcionales
                            uriBuilder.path("/api/transactions/products/{productId}")
                                    .queryParamIfPresent("startDate", Optional.ofNullable(startDate))
                                    .queryParamIfPresent("endDate", Optional.ofNullable(endDate));
                            return uriBuilder.build(productId);
                        })
                        .header("Authorization", token)
                        .retrieve()
                        .bodyToFlux(TransactionRS.class)
                        .doOnError(error -> log.error("Error while getTransactionsByProductId: {}", error.getMessage()))
                );
    }

    private Mono<TransactionRS> fallCreateTransaction(Mono<TransactionRQ> transactionRQMono, Throwable throwable) {
        log.error("Fallback for fallCreateTransaction {}, {}", JsonTransferUtil.objectToJson(transactionRQMono), throwable.getMessage());
        return Mono.just(new TransactionRS());
    }

    private Flux<TransactionRS> fallBackGetTransactionsByProductId(String productId, LocalDate startDate, LocalDate endDate,
                                                          Throwable throwable) {
        log.error("FallBack for fallBackGetTransactionsByProductId: {}, {}, {}, {}", productId, startDate, endDate, throwable.getMessage());
        return Flux.just(new TransactionRS());
    }

}
