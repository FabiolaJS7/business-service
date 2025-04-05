package com.bootcamp.business_service.service;

import com.bootcamp.business_service.constants.ProductTypeConstants;
import com.bootcamp.business_service.constants.TypeMovementConstants;
import com.bootcamp.commons.bean.products.InfoTransactionBean;
import org.springframework.stereotype.Component;

@Component
public class InfoTransactionManagement {

    public InfoTransactionBean buildToPassiveProduct(String productType) {
        InfoTransactionBean infoTransactionBean = new InfoTransactionBean();
        infoTransactionBean.setTransactionDone(TypeMovementConstants.NUM_TRANSACTION_DONE); //transacciones reaalizadas en 0 porque es nueva cuenta
        infoTransactionBean.setEnabledToMovement(true); //habilitado para transaction porque es nueva cuenta

        switch (productType) {
            case ProductTypeConstants.SAVING_ACCOUNT:
                //	Ahorro: libre de comisión por mantenimiento y con un límite máximo de movimientos mensuales.
                infoTransactionBean.setCommission(TypeMovementConstants.ZERO_COMMISSION_PER_MOVEMENT); //libre de comisión por mantenimiento
                infoTransactionBean.setMaxPerMonth(TypeMovementConstants.LIMIT_MAX_PER_MONTH_SAVING); //con un límite máximo de movimientos mensuales.
                return infoTransactionBean;
            case ProductTypeConstants.CURRENT_ACCOUNT:
                //	Cuenta corriente: posee comisión de mantenimiento y sin límite de movimientos mensuales.
                infoTransactionBean.setCommission(TypeMovementConstants.AMOUNT_COMMISSION_PER_MOVEMENT); // posee comisión de mantenimiento
                infoTransactionBean.setMaxPerMonth(TypeMovementConstants.FREE_MOVEMENT); //sin límite de movimientos mensuales.
                return infoTransactionBean;
            default:
                //	Plazo fijo: libre de comisión por mantenimiento, solo permite un movimiento de retiro o depósito en un día específico del mes
                infoTransactionBean.setCommission(TypeMovementConstants.ZERO_COMMISSION_PER_MOVEMENT); //libre de comisión por mantenimiento
                infoTransactionBean.setMaxPerMonth(TypeMovementConstants.LIMIT_MAX_PER_MONTH_FIXED); //solo permite un movimiento de retiro o depósito en un día específico del mes
                return infoTransactionBean;
        }
    }
}
