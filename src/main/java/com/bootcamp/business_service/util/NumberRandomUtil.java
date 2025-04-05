package com.bootcamp.business_service.util;

import com.bootcamp.business_service.constants.PrefixPassiveAccountConstants;
import reactor.core.publisher.Mono;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class NumberRandomUtil {

    public static String generateAccountNumber(String productType) {
        long randomNumber = ThreadLocalRandom.current().nextLong(1000000000L, 9999999999L); // Genera un número de 10 dígitos
        return PrefixPassiveAccountConstants.NUMBER_ACCOUNT.get(productType) + randomNumber; // Combina el prefijo con el número aleatorio
    }

    public static Mono<String> generateNumberCreditCard() {
        return Mono.fromSupplier(() -> IntStream.range(0, 4) // Genera 4 series
                .mapToObj(i -> String.valueOf(ThreadLocalRandom.current().nextInt(1000, 9999))) // cada serie con 4 dígitos randoms
                .collect(Collectors.joining("-"))); // Uniendo las series con guiones
    }
}
