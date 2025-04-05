package com.bootcamp.business_service.constants;

import java.util.ArrayList;

public class ProductTypeConstants {

    //PASSIVE PRODUCTS
    public static final String SAVING_ACCOUNT = "SA";
    public static final String CURRENT_ACCOUNT = "CA";
    public static final String FIXED_ACCOUNT = "FA";

    //ACTIVE PRODUCTS
    public static final String CREDIT_PERSONAL = "CP";
    public static final String CREDIT_BUSINESS = "CB";
    public static final String CREDIT_CARD = "CC";

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
