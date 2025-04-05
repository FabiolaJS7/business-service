package com.bootcamp.business_service.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Configuration
@Slf4j
public class WebClientCustomerApi {

    @Value("${client.customer.service}")
    String clientCustomer;

    @Value("${client.product.service}")
    String clientProduct;

    @Bean(name = "webClientCustomerService")
    public WebClient webClientCustomerService(WebClient.Builder builder) {
        return builder.baseUrl(clientCustomer)
                .filter(logRequest()) // Filtro para registrar la solicitud
                .filter(logResponse()) // Filtro para registrar la respuesta
                .build();
    }

    @Bean(name = "webClientProductService")
    public WebClient webClientProductService(WebClient.Builder builder) {
        return builder.baseUrl(clientProduct)
                .filter(logRequest()) // Filtro para registrar la solicitud
                .filter(logResponse()) // Filtro para registrar la respuesta
                .build();
    }


    private ExchangeFilterFunction logRequest() {
        return ExchangeFilterFunction.ofRequestProcessor(clientRequest -> {
            log.info("Request: {},  {}", clientRequest.method(), clientRequest.url());
            clientRequest.headers().forEach((name, values) -> values.forEach(value ->
                    log.error("{} : {}", name, value)
            ));
            return Mono.just(clientRequest);
        });
    }

    private ExchangeFilterFunction logResponse() {
        return ExchangeFilterFunction.ofResponseProcessor(clientResponse -> {
            log.info("Response status: {}", clientResponse.statusCode());
            return Mono.just(clientResponse);
        });
    }
}