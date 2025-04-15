package com.bootcamp.business_service.service.impl;

import com.bootcamp.business_service.connector.PlasticCardConnector;
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
import java.util.ArrayList;
import java.util.List;

@Service
@AllArgsConstructor
@Slf4j
public class ReportServiceImpl implements ReportService {

    private static final int LAST_10_MOVEMENTS = 10;

    TransactionConnector transactionConnector;
    ProductConnector productConnector;
    PlasticCardConnector plasticCardConnector;

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
                        return buildReportEmpty(reportRQ);
                    }

                    return reportRQ
                            .map(reportRQ1 -> {
                                ReportRS reportRS1 = new ReportRS();
                                reportRS1.setCustomerId(productReportBeans.get(0).getCustomerId());
                                reportRS1.setDateToday(LocalDate.now());
                                reportRS1.setTypeReport(reportRQ1.getTypeReport());
                                reportRS1.setProducts(productReportBeans);
                                reportRS1.setMessage("Success - Reporte productos del cliente ingresado.");
                                return reportRS1;
                            });

                })
                .doOnSuccess(reportRS1 -> log.info("Report successfully built {}", JsonTransferUtil.objectToJson(reportRS1)))
                .doOnError(throwable -> log.error("Request error getReportByCustomerId {}", throwable.getMessage()));

    }

    @Override // Ingresa a este método cuando typeReport es PLASTIC_CARD o MOVEMENT
    public Mono<ReportRS> getMovementByProductId(Mono<ReportRQ> reportRQ) {

        Flux<TransactionRS> transactionRSFlux = reportRQ
                .flatMapMany(rq -> {
                    if (rq.getTypeReport().equalsIgnoreCase(ReportTypeConstants.MOVEMENT)) { //condicional para cuando se quiere revisar reporte MOVEMENT por rango de fechas
                        return transactionConnector.getTransactionsByProductId(rq.getProductId(),
                                rq.getDateFrom(), rq.getDateTo());
                    } else { // Condicional cuando llega type_report PLASTIC_CARD para tomar los 10 últimos movimientos de la plasticcard
                        return plasticCardConnector.getPlasticCardById(rq.getPlasticCardId())
                                        .flatMap(plasticCardBean -> productConnector.getProductById(plasticCardBean.getProductIdAssociated()))
                                .flatMapMany(productResponse -> {
                                    if (Boolean.TRUE.equals(productResponse.getHasPlasticCard())) {
                                        return transactionConnector.getTransactionsByProductId(productResponse.getId(), null, null)
                                                .sort((m1, m2) -> m2.getDateOfTransaction()
                                                        .compareTo(m1.getDateOfTransaction())) // Ordenar por fecha desc
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
                        return buildReportEmpty(reportRQ);
                    } else {
                        return builtReportWhenHasMovements(reportRQ, movementReportbeans);
                    }
                })
                .doOnSuccess(reportRS1 -> log.info("Report of movement successfully built {}",
                        JsonTransferUtil.objectToJson(reportRS1)))
                .doOnError(throwable -> log.error("Request error getMovementByProductId {}", throwable.getMessage()));
    }

    private Mono<ReportRS> builtReportWhenHasMovements(Mono<ReportRQ> reportRQ, List<MovementReportbean> movementReportbeans) {
        return reportRQ
                .flatMap(rq -> {
                    ReportRS reportRS1 = new ReportRS();
                    reportRS1.setCustomerId(rq.getCustomerId());
                    reportRS1.setDateToday(LocalDate.now());
                    reportRS1.setTypeReport(rq.getTypeReport());

                    ResumeMovement resumeMovement = new ResumeMovement();
                    resumeMovement.setProductId(rq.getProductId());
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

                    if (rq.getTypeReport().equalsIgnoreCase(ReportTypeConstants.PLASTIC_CARD)) {
                        return plasticCardConnector.getPlasticCardById(rq.getPlasticCardId())
                                .flatMap(plasticCardBean -> productConnector.findBalanceByProductId(plasticCardBean.getProductIdAssociated())
                                        .map(balanceBeanResponse -> {
                                            List<ProductReportbean> products = new ArrayList<>();
                                            ProductReportbean productReportbean = new ProductReportbean();
                                            productReportbean.setProductId(plasticCardBean.getProductIdAssociated());
                                            productReportbean.setCreditCardNumber(plasticCardBean.getCardNumber());
                                            productReportbean.setProductType(plasticCardBean.getCardType());
                                            productReportbean.setCreditUser(balanceBeanResponse.getCreditLimitUsed());
                                            productReportbean.setCreditLimitTotal(balanceBeanResponse.getCreditLimit());
                                            productReportbean.setCreditEnabled(balanceBeanResponse.getCreditEnabledToUse());
                                            products.add(productReportbean);
                                            reportRS1.setProducts(products);
                                            reportRS1.setMessage("Success - Reporte últimos 10 movimientos del plastic card associated to productId "
                                                    + plasticCardBean.getProductIdAssociated());
                                            return reportRS1;
                                        }).thenReturn(reportRS1)
                                );


                    } else {
                        return productConnector.getProductById(rq.getProductId())
                                .flatMap(productResponse -> productConnector.findBalanceByProductId(rq.getProductId())
                                        .map(balanceBeanResponse -> {
                                            List<ProductReportbean> products = new ArrayList<>();
                                            ProductReportbean productReportbean = new ProductReportbean();
                                            productReportbean.setProductId(productResponse.getId());
                                            productReportbean.setCreditCardNumber(productResponse.getCardNumber());
                                            productReportbean.setCustomerId(productResponse.getCustomer().getCustomerId());
                                            productReportbean.setProductType(ProductTypeConstants.COMPLETE_PRODUCTS_NAME
                                                    .get(productResponse.getProductType()));
                                            productReportbean.setBalance(balanceBeanResponse.getTotalAmountInAccount());
                                            productReportbean.setCreditEnabled(balanceBeanResponse.getCreditEnabledToUse());
                                            productReportbean.setCreditLimitTotal(balanceBeanResponse.getCreditLimit());
                                            productReportbean.setCreditUser(balanceBeanResponse.getCreditLimitUsed());
                                            products.add(productReportbean);
                                            reportRS1.setProducts(products);
                                            reportRS1.setMessage("Success - Reporte movimientos realizados por el producto indicado");
                                            return reportRS1;
                                        }));

                    }
                });
    }

    private Mono<ReportRS> buildReportEmpty(Mono<ReportRQ> reportRQ) {
        return reportRQ
                .map(rq -> {
                    ReportRS rs = new ReportRS();
                    if (rq.getTypeReport().equalsIgnoreCase(ReportTypeConstants.PLASTIC_CARD)) {
                        rs.setMessage("La producto plastic card ingresado no cuenta con movimientos o no es una plastic card");
                        rs.setCustomerId(rq.getCustomerId());
                        rs.setDateToday(LocalDate.now());
                        return rs;

                    }  else if (rq.getTypeReport().equalsIgnoreCase(ReportTypeConstants.BALANCE)) {
                        rs.setMessage("no existen productos para el cliente ingresado");
                        rs.setCustomerId(rq.getCustomerId());
                        rs.setDateToday(LocalDate.now());
                        return rs;
                    } else {
                        rs.setMessage("El producto ingresado no cuenta con movimientos");
                        rs.setCustomerId(rq.getCustomerId());
                        rs.setDateToday(LocalDate.now());
                        return rs;
                    }

                });
    }

    private Double getByConcept(List<MovementReportbean> movementReportbeans, String movementType) {
        return movementReportbeans
                .stream()
                .filter(movementReportbean -> movementType.equalsIgnoreCase(movementReportbean.getMovementType()))
                .map(MovementReportbean::getAmount)
                .reduce(0.00, Double::sum);
    }
}
