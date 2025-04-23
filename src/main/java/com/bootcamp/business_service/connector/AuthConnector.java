package com.bootcamp.business_service.connector;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.beans.factory.annotation.Qualifier;
import reactor.core.publisher.Mono;

import java.util.Map;

@Service
@Slf4j
public class AuthConnector {

    private final WebClient webClient;

    public AuthConnector(@Qualifier("webClientService") WebClient webClient) {
        this.webClient = webClient;
    }

    // Método para obtener el token desde /auth/register
    public Mono<String> getAuthToken() {
        log.info("Requesting token from /auth/register");

        // Credenciales para autenticación
        Map<String, String> credentials = Map.of(
                "email", "user@example.com",
                "password", "password",
                "name", "user"
        );

        return webClient.post()
                .uri("/auth/register") // URL del servicio de autenticación
                .bodyValue(credentials)
                .retrieve()
                .bodyToMono(Map.class) // El token se devuelve como un mapa
                .map(response -> (String) response.get("accessToken")) // Extraiendo el token
                .doOnNext(token -> log.info("Received token: {}", token))
                .doOnError(error -> log.error("Error getting token: {}", error.getMessage()));
    }
}
