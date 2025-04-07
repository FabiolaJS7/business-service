package com.bootcamp.business_service.service;

import com.bootcamp.business_service.model.MovementRQ;
import com.bootcamp.business_service.model.MovementRS;
import reactor.core.publisher.Mono;

public interface MovementService {

    Mono<MovementRS> doMovementToTransaction(Mono<MovementRQ> movementRQMono);
}
