package com.bootcamp.business_service.service;

import com.bootcamp.business_service.model.ReportRQ;
import com.bootcamp.business_service.model.ReportRS;
import reactor.core.publisher.Mono;

public interface ReportService {
    Mono<ReportRS> getReportByCustomerId(Mono<ReportRQ> reportRQ);
}
