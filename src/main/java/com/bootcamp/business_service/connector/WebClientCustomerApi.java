package com.bootcamp.business_service.connector;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class WebClientCustomerApi {

    @Value("${client.customer.service}")
    String clientCustomer;

    @Bean
    public WebClient webClientCustomerService(WebClient.Builder builder) {
        return builder.baseUrl(clientCustomer)
                .build();
    }
}