package com.bootcamp.business_service.expose;

import com.bootcamp.business_service.api.ApiApiDelegate;
import com.bootcamp.business_service.constants.ReportTypeConstants;
import com.bootcamp.business_service.model.*;
import com.bootcamp.business_service.service.CustomerService;
import com.bootcamp.business_service.service.MovementService;
import com.bootcamp.business_service.service.ProductService;
import com.bootcamp.business_service.service.ReportService;
import com.bootcamp.business_service.util.JsonTransferUtil;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Service
@Slf4j
@AllArgsConstructor
public class BusinessDelegateImpl implements ApiApiDelegate {

    MovementService movementService;
    ProductService productService;
    ReportService reportService;
    CustomerService customerService;

    @Override
    public Mono<ResponseEntity<CreateProductRS>> createProduct(Mono<CreateProductRQ> createProductRQ,
                                                                ServerWebExchange exchange) {
        return createProductRQ
                .doOnNext(c -> log.info("-> Init createProduct."))
                .flatMap(c -> productService.createProduct(Mono.just(c)))
                .map(ResponseEntity::ok)
                .doOnSuccess(createProductRS -> log.info("End createProduct."))
                .doOnError(throwable -> log.error("Request error createProduct {}", throwable.getMessage()))
                .onErrorResume(e -> Mono.just(new ResponseEntity<>(HttpStatus.BAD_REQUEST)));

    }

    @Override
    public Mono<ResponseEntity<AdditionalPersonRS>> additionalPerson(Mono<AdditionalPersonRQ> additionalPersonRQ,
                                                                      ServerWebExchange exchange) {
        return additionalPersonRQ
                .doOnNext(a -> log.info("-> Init additionalPerson: {}", JsonTransferUtil.objectToJson(a)))
                .flatMap(a -> productService.updateAdditionalPerson(Mono.just(a)))
                .map(ResponseEntity::ok)
                .doOnSuccess(a -> log.info("success update additionalperson {}", JsonTransferUtil.objectToJson(a)))
                .doOnError(throwable -> log.error("Request error updateAdditionalPerson {}", throwable.getMessage()))
                .onErrorResume(e -> Mono.just(new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR)));

    }

    @Override
    public Mono<ResponseEntity<MovementRS>> doMovement(Mono<MovementRQ> movementRQ, ServerWebExchange exchange) {
        return  movementRQ
                .doOnNext(m -> log.info("-> Init doMovement."))
                .flatMap(m -> movementService.doMovementToTransaction(Mono.just(m)))
                .map(ResponseEntity::ok)
                .doOnSuccess(movement -> log.info("success doMovement."))
                .doOnError(throwable -> log.error("Request error doMovement {}", throwable.getMessage()))
                .onErrorResume(e -> Mono.just(new ResponseEntity<>(HttpStatus.BAD_REQUEST)));
    }

    @Override
    public Mono<ResponseEntity<ReportRS>> buildReport(Mono<ReportRQ> reportRQ, ServerWebExchange exchange) {
        return reportRQ
                .flatMap(reportRQ1 -> {
                    if (reportRQ1.getTypeReport().equalsIgnoreCase(ReportTypeConstants.BALANCE)) {
                        return reportService.getReportByCustomerId(Mono.just(reportRQ1));
                    } else {
                        return reportService.getMovementByProductId(Mono.just(reportRQ1));
                    }
                })
                .map(ResponseEntity::ok)
                .doOnError(throwable -> log.error("Request error buildReport {}", throwable.getMessage()));
    }

    @Override
    public Mono<ResponseEntity<ManagementCustomerRS>> managementCustomer(Mono<ManagementCustomerRQ> managementCustomerRQ,
                                                                          ServerWebExchange exchange) {
        log.info("-> Management customer RQ: {}", managementCustomerRQ);
        return managementCustomerRQ
                .flatMap(customer -> customerService.managementCustomer(Mono.just(customer)))
                .map(ResponseEntity::ok)
                .doOnError(throwable -> log.error("Request error management {}", throwable.getMessage()));

    }

    @Override
    public Mono<ResponseEntity<CreateCardRS>> createCard(Mono<CreateCardRQ> createCardRQ, ServerWebExchange exchange) {
        log.info("-> Init createCard DEBIT to passive product");
        return createCardRQ
                .flatMap(rq -> productService.createCardToPassiveProduct(Mono.just(rq)))
                .map(ResponseEntity::ok)
                .doOnError(throwable -> log.error("Request error createCard {}", throwable.getMessage()))
                .onErrorResume(e -> Mono.just(new ResponseEntity<>(HttpStatus.BAD_REQUEST)));
    }

}
