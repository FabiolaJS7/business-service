package com.bootcamp.business_service.mapper;

import com.bootcamp.business_service.model.ManagementCustomerRQ;
import com.bootcamp.commons.bean.customers.CustomerRequest;
import org.mapstruct.Mapper;
import org.mapstruct.factory.Mappers;

@Mapper
public interface CustomerMapperStruct {

    CustomerMapperStruct INSTANCE = Mappers.getMapper(CustomerMapperStruct.class);

    // Mapea ManagementCustomerRQ a CustomerRequest
    CustomerRequest toCustomerRequestOfManagementCustomerRq(ManagementCustomerRQ managementCustomerRQ);
}
