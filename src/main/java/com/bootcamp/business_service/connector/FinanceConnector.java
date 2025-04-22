package com.bootcamp.business_service.connector;

import com.bootcamp.commons.bean.finance.ResumeResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.time.LocalDate;
import java.util.Optional;

@Service
@Slf4j
public class FinanceConnector {

    WebClient webClient;

    public FinanceConnector(@Qualifier("webClientService") WebClient webClient) {
        this.webClient = webClient;
    }

    public Flux<ResumeResponse> getResumesByProductId(String productId, LocalDate startDate, LocalDate endDate) {
        log.info("API Get Resumes By ProductId RQ: {} and dates from {}, to {}", productId, startDate, endDate);
        return webClient.get()
                .uri(uriBuilder -> {
                    uriBuilder.path("/api/finance/resumes/{productId}")
                            .queryParamIfPresent("startDate", Optional.ofNullable(startDate))
                            .queryParamIfPresent("endDate", Optional.ofNullable(endDate));
                    return uriBuilder.build(productId);
                })
                .retrieve()
                .bodyToFlux(ResumeResponse.class)
                .doOnNext(resumeResponse -> log.info("API get Resumes By ProductId success RS: {}", productId))
                .doOnError(throwable -> log.error("API error get Resumes By ProductId {}", throwable.getMessage()));
    }
}
