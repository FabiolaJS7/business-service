package com.bootcamp.business_service.util;

import com.bootcamp.business_service.constants.PrefixPassiveAccountConstants;
import reactor.core.publisher.Mono;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class NumberRandomUtil {

    private static  final long ORIGIN_RANDOM_NUMBER_LENGTH = 1000000000L;
    private static  final long END_RANDOM_NUMBER_LENGTH = 1000000000L;

    private static final int ORIGIN_SERIES_RAMD_LENGTH = 1000;
    private static final int END_SERIES_RAMD_LENGTH = 9999;

    private static final int RANGE_0 = 0;
    private static final int QUANTITY_RANGE = 4;



    public static String generateAccountNumber(String productType) {
        long randomNumber = ThreadLocalRandom.current().nextLong(ORIGIN_RANDOM_NUMBER_LENGTH, END_RANDOM_NUMBER_LENGTH); // Genera un número de 10 dígitos
        return PrefixPassiveAccountConstants.NUMBER_ACCOUNT.get(productType) + randomNumber; // Combina el prefijo con el número aleatorio
    }

    public static Mono<String> generateNumberCreditCard() {
        return Mono.fromSupplier(() -> IntStream.range(RANGE_0, QUANTITY_RANGE) // Genera 4 series
                .mapToObj(i -> String.valueOf(ThreadLocalRandom.current().nextInt(ORIGIN_SERIES_RAMD_LENGTH, END_SERIES_RAMD_LENGTH))) // cada serie con 4 dígitos randoms
                .collect(Collectors.joining("-"))); // Uniendo las series con guiones
    }
}
