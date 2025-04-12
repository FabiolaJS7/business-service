package com.bootcamp.business_service.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.File;
import java.io.IOException;

public class JsonTransferUtil {

    public static String objectToJson(Object object) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);

        // Registrar el módulo para soportar Java 8 LocalDate y otros tipos de fecha/hora
        mapper.registerModule(new JavaTimeModule());

        String jsonString = null;
        try {
            jsonString = mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return jsonString;
    }

    public static <T> T getObjectFromJSONFile(Class<T> type, String filePath) {
        ClassLoader classLoader = type.getClassLoader();
        File file = new File(classLoader.getResource(filePath).getFile());
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        T object = null;

        try {
            object = mapper.readValue(file, type);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return object;
    }
}
