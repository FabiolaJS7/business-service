package com.bootcamp.business_service.constants;

import java.util.ArrayList;

public class CustomerTypeConstants {

    public static final String PERSONAL = "P";
    public static final String BUSINESS = "B";
    public static final String PERSONAL_VIP = "V";
    public static final String BUSINESS_PYME = "M";

    public static final ArrayList<String> CUSTOMER_BUSINESS = new ArrayList<String>();

    static {
        CUSTOMER_BUSINESS.add(BUSINESS);
        CUSTOMER_BUSINESS.add(BUSINESS_PYME);
    }


}
