package com.bootcamp.business_service.constants;

import java.util.ArrayList;
import java.util.HashMap;

public class ProductTypeConstants {

    //PASSIVE PRODUCTS
    public static final String SAVING_ACCOUNT = "SA"; //Cuenta de ahorros
    public static final String CURRENT_ACCOUNT = "CA"; //Cuenta corriente
    public static final String FIXED_ACCOUNT = "FA"; //Plazo fijo
    public static final String CURRENT_ACCOUNT_PYME = "CAM"; //Cuenta corriente

    //ACTIVE PRODUCTS
    public static final String CREDIT_PERSONAL = "CP"; //Credito Personal
    public static final String CREDIT_BUSINESS = "CB"; //Credito empresa
    public static final String CREDIT_CARD = "CC"; //Tarjeta de credito

    public static final ArrayList<String> PASSIVE_PRODUCTS = new ArrayList<String>();

    static {
        PASSIVE_PRODUCTS.add(SAVING_ACCOUNT);
        PASSIVE_PRODUCTS.add(CURRENT_ACCOUNT);
        PASSIVE_PRODUCTS.add(FIXED_ACCOUNT);
        PASSIVE_PRODUCTS.add(CURRENT_ACCOUNT_PYME);
    }

    public static final ArrayList<String> ACTIVE_PRODUCTS = new ArrayList<String>();

    static {
        ACTIVE_PRODUCTS.add(CREDIT_PERSONAL);
        ACTIVE_PRODUCTS.add(CREDIT_BUSINESS);
        ACTIVE_PRODUCTS.add(CREDIT_CARD);
    }

    public static final HashMap<String, String> COMPLETE_PRODUCTS_NAME = new HashMap<>();

    static {
        COMPLETE_PRODUCTS_NAME.put(SAVING_ACCOUNT, "CUENTA DE AHORROS");
        COMPLETE_PRODUCTS_NAME.put(CURRENT_ACCOUNT, "CUENTA CORRIENTE");
        COMPLETE_PRODUCTS_NAME.put(FIXED_ACCOUNT, "PLAZO FIJO");
        COMPLETE_PRODUCTS_NAME.put(CREDIT_PERSONAL, "CREDITO PERSONAL");
        COMPLETE_PRODUCTS_NAME.put(CREDIT_BUSINESS, "CREDITO NEGOCIO");
        COMPLETE_PRODUCTS_NAME.put(CREDIT_CARD, "TARJETA DE CREDITO");
        COMPLETE_PRODUCTS_NAME.put(CURRENT_ACCOUNT_PYME, "CURRENT ACCOUNT BUSINESS PYME");

    }



}
