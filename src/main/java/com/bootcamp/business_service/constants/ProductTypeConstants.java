package com.bootcamp.business_service.constants;

import java.util.ArrayList;

public class ProductTypeConstants {

    //PASSIVE PRODUCTS
    public static final String SAVING_ACCOUNT = "SA"; //Cuenta de ahorros
    public static final String CURRENT_ACCOUNT = "CA"; //Cuenta corriente
    public static final String FIXED_ACCOUNT = "FA"; //Plazo fijo

    //ACTIVE PRODUCTS
    public static final String CREDIT_PERSONAL = "CP"; //Credito Personal
    public static final String CREDIT_BUSINESS = "CB"; //Credito empresa
    public static final String CREDIT_CARD = "CC"; //Tarjeta de credito

    public static final ArrayList<String> PASSIVE_PRODUCTS = new ArrayList<String>();

    static {
        PASSIVE_PRODUCTS.add(SAVING_ACCOUNT);
        PASSIVE_PRODUCTS.add(CURRENT_ACCOUNT);
        PASSIVE_PRODUCTS.add(FIXED_ACCOUNT);
    }

    public static final ArrayList<String> ACTIVE_PRODUCTS = new ArrayList<String>();

    static {
        ACTIVE_PRODUCTS.add(CREDIT_PERSONAL);
        ACTIVE_PRODUCTS.add(CREDIT_BUSINESS);
        ACTIVE_PRODUCTS.add(CREDIT_CARD);
    }



}
