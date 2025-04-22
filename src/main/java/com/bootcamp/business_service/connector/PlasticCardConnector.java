package com.bootcamp.business_service.connector;

import com.bootcamp.business_service.util.JsonTransferUtil;
import com.bootcamp.commons.bean.products.PlasticCardBean;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class PlasticCardConnector {

    public static final String MAIN_PATH_PRODUCT = "/api/products/";
    WebClient webClient;

    public PlasticCardConnector(@Qualifier("webClientService") WebClient webClient) {
        this.webClient = webClient;
    }

    @CircuitBreaker(name = "plasticCardService", fallbackMethod = "fallbackGetPlasticCardById")
    public Mono<PlasticCardBean> getPlasticCardById(String cardId) {
        log.info("API getPlasticCardById RQ: {}", cardId);
        return webClient.get()
                .uri(MAIN_PATH_PRODUCT + "cards/" + cardId)
                .retrieve()
                .bodyToMono(PlasticCardBean.class)
                .doOnNext(plasticCardBean -> log.info("API getPlasticCardById RS: {}",
                        JsonTransferUtil.objectToJson(plasticCardBean)))
                .doOnError(error -> log.error("Error API while getting product by card: {}", error.getMessage()));
    }

    private Mono<PlasticCardBean> fallbackGetPlasticCardById(String cardId, Throwable throwable) {
        log.error("Fallback por fallbackGetPlasticCardById {}, {}", cardId, throwable.getMessage());
        return Mono.just(new PlasticCardBean());
    }
}
