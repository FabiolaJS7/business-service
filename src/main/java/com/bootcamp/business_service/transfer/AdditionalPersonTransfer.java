package com.bootcamp.business_service.transfer;

import com.bootcamp.business_service.constants.CasesUpdateConstants;
import com.bootcamp.business_service.model.AdditionalPersonRQ;
import com.bootcamp.commons.bean.products.AdditionalPersonBean;
import com.bootcamp.commons.bean.products.IdentificationBean;
import com.bootcamp.commons.bean.products.ProductUpdateRQ;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

@Component
@Slf4j
@AllArgsConstructor
public class AdditionalPersonTransfer {

    public Mono<ProductUpdateRQ> buildProductUpdateRQ(Mono<AdditionalPersonRQ> additionalPersonRQMono) {

        return additionalPersonRQMono.flatMap(additionalPersonRQ -> {
            ProductUpdateRQ productUpdateRQ = new ProductUpdateRQ();
            if (additionalPersonRQ.getTypeAdditional().equalsIgnoreCase("HOLD")) {
                productUpdateRQ.setActionToUpdate(CasesUpdateConstants.CHANGE_HOLDERS);
                productUpdateRQ.setHolders(this.buildAdditionalPersonBean(additionalPersonRQ));
            } else {
                productUpdateRQ.setActionToUpdate(CasesUpdateConstants.CHANGE_SIGNATURES);
                productUpdateRQ.setAuthorizedSignatories(this.buildAdditionalPersonBean(additionalPersonRQ));
            }

            return Mono.just(productUpdateRQ);
        });
    }

    private List<AdditionalPersonBean> buildAdditionalPersonBean(AdditionalPersonRQ additionalPersonRQ) {
        List<AdditionalPersonBean> additionalPersonBeans = new ArrayList<>();
        AdditionalPersonBean additionalPersonBean = new AdditionalPersonBean();
        additionalPersonBean.setFullName(additionalPersonRQ.getFullName());
        additionalPersonBean.setEmail(additionalPersonRQ.getEmail());
        additionalPersonBean.setPhone(additionalPersonRQ.getPhone());


        IdentificationBean identificationBean = new IdentificationBean();
        identificationBean.setNumberIdentification(additionalPersonRQ.getIdentificationNum());
        identificationBean.setTypeIdentification(additionalPersonRQ.getIdentificationType());
        additionalPersonBean.setIdentification(identificationBean);
        additionalPersonBeans.add(additionalPersonBean);
        return additionalPersonBeans;
    }


}
