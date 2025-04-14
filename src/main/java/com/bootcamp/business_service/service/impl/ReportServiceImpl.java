package com.bootcamp.business_service.service.impl;

import com.bootcamp.business_service.connector.ProductConnector;
import com.bootcamp.business_service.connector.TransactionConnector;
import com.bootcamp.business_service.constants.MovementTypeConstants;
import com.bootcamp.business_service.constants.ProductTypeConstants;
import com.bootcamp.business_service.model.*;
import com.bootcamp.business_service.service.ReportService;
import com.bootcamp.business_service.util.JsonTransferUtil;
import com.bootcamp.commons.bean.products.ProductResponse;
import com.bootcamp.commons.bean.transaction.TransactionRS;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDate;

@Service
@AllArgsConstructor
@Slf4j
public class ReportServiceImpl implements ReportService {


    TransactionConnector transactionConnector;
    ProductConnector productConnector;

    @Override
    public Mono<ReportRS> getReportByCustomerId(Mono<ReportRQ> reportRQ) {

        Flux<ProductResponse> productResponseFlux = reportRQ
                .flatMapMany(reportRQ1 -> productConnector.getProductsByCustomerId(reportRQ1.getCustomerId()));

        Flux<ProductReportbean> productReportbeanFlux = productResponseFlux
                .flatMap(productResponses ->

                        productConnector.findBalanceByProductId(productResponses.getId())
                                .map(balanceBeanResponse -> {
                                    ProductReportbean productReportbean = new ProductReportbean();
                                    productReportbean.setProductId(productResponses.getId());
                                    productReportbean.setProductType(ProductTypeConstants.COMPLETE_PRODUCTS_NAME.get(productResponses.getProductType()));
                                    productReportbean.setAccountNumber(productResponses.getAccountNumber());
                                    productReportbean.setCustomerId(productResponses.getCustomer().getCustomerId());
                                    productReportbean.setFamilyType(ProductTypeConstants.PASSIVE_PRODUCTS.contains(productResponses.getProductType()) ? "PASSIVE" : "ACTIVE");
                                    productReportbean.setCreditLimitTotal(balanceBeanResponse.getCreditLimit());
                                    productReportbean.setCreditUser(balanceBeanResponse.getCreditLimitUsed());
                                    productReportbean.setCreditEnabled(balanceBeanResponse.getCreditEnabledToUse());
                                    productReportbean.setBalance(balanceBeanResponse.getTotalAmountInAccount());
                                    return productReportbean;
                                })
                );


        return productReportbeanFlux
                .collectList()
                .flatMap(productReportBeans -> {

                    if (productReportBeans.isEmpty()) {
                        // Manejar el caso en que no haya productos
                        return Mono.error(new RuntimeException("No products found for the given customer"));
                    }

                    return reportRQ
                            .map(reportRQ1 -> {
                                ReportRS reportRS1 = new ReportRS();
                                reportRS1.setCustomerId(productReportBeans.get(0).getCustomerId());
                                reportRS1.setDateToday(LocalDate.now());
                                reportRS1.setTypeReport(reportRQ1.getTypeReport());
                                reportRS1.setProducts(productReportBeans);
                                return reportRS1;
                            });

                })
                .doOnSuccess(reportRS1 -> log.info("Report successfully built {}", JsonTransferUtil.objectToJson(reportRS1)))
                .doOnError(throwable -> log.error("Request error getReportByCustomerId {}", throwable.getMessage()));

    }

    @Override
    public Mono<ReportRS> getMovementByProductId(Mono<ReportRQ> reportRQ) {

        Flux<TransactionRS> transactionRSFlux = reportRQ
                .flatMapMany(rq -> transactionConnector.getTransactionsByProductId(rq.getProductId(),
                        rq.getDateFrom(), rq.getDateTo()));

        Flux<MovementReportbean> movementReportbeanFlux = transactionRSFlux
                .map(transactionRS -> {
                    MovementReportbean movementReportbean = new MovementReportbean();
                    movementReportbean.setAmount(transactionRS.getAmount());
                    movementReportbean.setMovementType(transactionRS.getMovementType());
                    movementReportbean.setDateOfMovement(transactionRS.getDateOfTransaction());
                    movementReportbean.setCommissionAmount(transactionRS.getCommissionAmount());
                    return movementReportbean;
                });

        return movementReportbeanFlux
                .collectList()
                .flatMap(movementReportbeans -> {
                    if (movementReportbeans.isEmpty()) {
                        // Manejar el caso en que no haya transactions
                        return Mono.error(new RuntimeException("No transactions found for this product"));
                    }

                    return reportRQ
                            .map(reportRQ1 -> {
                                ReportRS reportRS1 = new ReportRS();
                                reportRS1.setCustomerId(reportRQ1.getCustomerId());
                                reportRS1.setDateToday(LocalDate.now());
                                reportRS1.setTypeReport(reportRQ1.getTypeReport());
                                ResumeMovement resumeMovement = new ResumeMovement();
                                resumeMovement.setProductId(reportRQ1.getProductId());
                                resumeMovement.setTotalAmountCommission(movementReportbeans
                                        .stream()
                                        .map(MovementReportbean::getCommissionAmount)
                                        .reduce(0.00, Double::sum));
                                resumeMovement.setTotalAmountConsume(movementReportbeans
                                        .stream()
                                        .filter(movementReportbean -> MovementTypeConstants.CONSUME.equalsIgnoreCase(movementReportbean.getMovementType()))
                                        .map(MovementReportbean::getAmount)
                                        .reduce(0.00, Double::sum));
                                resumeMovement.setTotalAmountDeposit(movementReportbeans
                                        .stream()
                                        .filter(movementReportbean -> MovementTypeConstants.DEPOSIT.equalsIgnoreCase(movementReportbean.getMovementType()))
                                        .map(MovementReportbean::getAmount)
                                        .reduce(0.00, Double::sum));
                                resumeMovement.setTotalAmountWithdraw(movementReportbeans
                                        .stream()
                                        .filter(movementReportbean -> MovementTypeConstants.WITHDRAW.equalsIgnoreCase(movementReportbean.getMovementType()))
                                        .map(MovementReportbean::getAmount)
                                        .reduce(0.00, Double::sum));
                                resumeMovement.setTotalAmountPayments(movementReportbeans
                                        .stream()
                                        .filter(movementReportbean -> MovementTypeConstants.PAYMENT.equalsIgnoreCase(movementReportbean.getMovementType()))
                                        .map(MovementReportbean::getAmount)
                                        .reduce(0.00, Double::sum));
                                resumeMovement.setMovements(movementReportbeans);
                                reportRS1.setResumeMovement(resumeMovement);
                                return reportRS1;
                            });
                })
                .doOnSuccess(reportRS1 -> log.info("Report of movement successfully built {}", JsonTransferUtil.objectToJson(reportRS1)))
                .doOnError(throwable -> log.error("Request error getMovementByProductId {}", throwable.getMessage()));
    }

}
