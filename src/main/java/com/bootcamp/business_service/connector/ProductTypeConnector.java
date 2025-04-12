package com.bootcamp.business_service.connector;

import com.bootcamp.business_service.util.JsonTransferUtil;
import com.bootcamp.commons.bean.products.ProductResponse;
import com.bootcamp.commons.bean.products.ProductTypeResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class ProductTypeConnector {

    WebClient webClient;

    public ProductTypeConnector(@Qualifier("webClientProductService") WebClient webClient) {
        this.webClient = webClient;
    }

    public Mono<ProductTypeResponse> getProductTypeByCode(String code) {
        log.info("API getProductByCustomerId RQ: {}", code);
        return webClient.get()
                .uri("/api/products/types/" + code)
                .retrieve()
                .bodyToMono(ProductTypeResponse.class)
                .doOnNext(productTypeResponse -> log.info("API getProductTypeByCode RQ: {}",
                        JsonTransferUtil.objectToJson(productTypeResponse)))
                .doOnError(throwable -> log.error("API error getProductTypeByCode: {}", throwable.getMessage()));
    }
}
