package com.bootcamp.business_service.util;

import com.bootcamp.business_service.constants.PrefixPassiveAccountConstants;

import java.util.concurrent.ThreadLocalRandom;

public class NumberAccountRandom {

    public static String generateAccountNumber(String productType) {
        long randomNumber = ThreadLocalRandom.current().nextLong(1000000000L, 9999999999L); // Genera un número de 10 dígitos
        return PrefixPassiveAccountConstants.NUMBER_ACCOUNT.get(productType) + randomNumber; // Combina el prefijo con el número aleatorio
    }
}
