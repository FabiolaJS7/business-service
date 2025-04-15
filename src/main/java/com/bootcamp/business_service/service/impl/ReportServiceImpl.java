package com.bootcamp.business_service.service.impl;

import com.bootcamp.business_service.connector.ProductConnector;
import com.bootcamp.business_service.connector.TransactionConnector;
import com.bootcamp.business_service.constants.MovementTypeConstants;
import com.bootcamp.business_service.constants.ProductTypeConstants;
import com.bootcamp.business_service.constants.ReportTypeConstants;
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
import java.util.List;

@Service
@AllArgsConstructor
@Slf4j
public class ReportServiceImpl implements ReportService {

    private static final int LAST_10_MOVEMENTS = 10;
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
                .flatMapMany(rq -> {
                    if (rq.getTypeReport().equalsIgnoreCase(ReportTypeConstants.MOVEMENT)) { //condicional para cuando se quiere revisar reporte MOVEMENT por rango de fechas
                        return transactionConnector.getTransactionsByProductId(rq.getProductId(),
                                rq.getDateFrom(), rq.getDateTo());
                    } else { // Condicional cuando llega type_report PLASTIC_CARD para tomar los 10 últimos movimientos de la plasticcard
                        return productConnector.getProductById(rq.getProductId())
                                .flatMapMany(productResponse -> {
                                    if (Boolean.TRUE.equals(productResponse.getHasPlasticCard())) {
                                        return transactionConnector.getTransactionsByProductId(rq.getProductId(), null, null)
                                                .sort((m1, m2) -> m2.getDateOfTransaction().compareTo(m1.getDateOfTransaction())) // Ordenar por fecha desc
                                                .take(LAST_10_MOVEMENTS); // Toma los últimos 10 registros más recientes
                                    }

                                    return Flux.empty();
                                });
                    }
                });

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
                        return reportRQ
                                .flatMap(rq -> {
                                    ReportRS rs = new ReportRS();
                                   if (rq.getTypeReport().equalsIgnoreCase(ReportTypeConstants.PLASTIC_CARD)) {

                                       rs.setCustomerId(rq.getCustomerId());
                                       rs.setDateToday(LocalDate.now());
                                       return Mono.just(rs);

                                   }

                                   return Mono.just(rs);
                                });
                    } else {
                        return reportRQ
                                .map(reportRQ1 -> {
                                    ReportRS reportRS1 = new ReportRS();
                                    reportRS1.setCustomerId(reportRQ1.getCustomerId());
                                    reportRS1.setDateToday(LocalDate.now());
                                    reportRS1.setTypeReport(reportRQ1.getTypeReport());
                                    reportRS1.setMessage("success");
                                    ResumeMovement resumeMovement = new ResumeMovement();
                                    resumeMovement.setProductId(reportRQ1.getProductId());
                                    resumeMovement.setTotalAmountCommission(movementReportbeans
                                            .stream()
                                            .map(MovementReportbean::getCommissionAmount)
                                            .reduce(0.00, Double::sum));
                                    resumeMovement.setTotalAmountConsume(getByConcept(movementReportbeans,
                                            MovementTypeConstants.CONSUME));
                                    resumeMovement.setTotalAmountDeposit(getByConcept(movementReportbeans,
                                            MovementTypeConstants.DEPOSIT));
                                    resumeMovement.setTotalAmountWithdraw(getByConcept(movementReportbeans,
                                            MovementTypeConstants.WITHDRAW));
                                    resumeMovement.setTotalAmountPayments(getByConcept(movementReportbeans,
                                            MovementTypeConstants.PAYMENT));
                                    resumeMovement.setMovements(movementReportbeans);
                                    reportRS1.setResumeMovement(resumeMovement);
                                    return reportRS1;
                                });
                    }


                })
                .doOnSuccess(reportRS1 -> log.info("Report of movement successfully built {}", JsonTransferUtil.objectToJson(reportRS1)))
                .doOnError(throwable -> log.error("Request error getMovementByProductId {}", throwable.getMessage()));
    }

    private Double getByConcept(List<MovementReportbean> movementReportbeans, String movementType) {
        return movementReportbeans
                .stream()
                .filter(movementReportbean -> movementType.equalsIgnoreCase(movementReportbean.getMovementType()))
                .map(MovementReportbean::getAmount)
                .reduce(0.00, Double::sum);
    }
}
