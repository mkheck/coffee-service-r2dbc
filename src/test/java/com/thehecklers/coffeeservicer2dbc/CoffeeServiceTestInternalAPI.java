package com.thehecklers.coffeeservicer2dbc;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.junit.Assert.*;

@WebFluxTest(CoffeeService.class)
@RunWith(SpringRunner.class)
public class CoffeeServiceTestInternalAPI {
    @Autowired
    private CoffeeService service;

    @MockBean
    private CoffeeRepository repo;

    private Coffee coffee1, coffee2;

    @Before
    public void setup() {
        coffee1 = new Coffee("1", "Tester's Choice");
        coffee2 = new Coffee("2", "Maxfail House");

        Mockito.when(repo.findAll()).thenReturn(Flux.just(coffee1, coffee2));
        Mockito.when(repo.findById(coffee1.getId())).thenReturn(Mono.just(coffee1));
        Mockito.when(repo.findById(coffee2.getId())).thenReturn(Mono.just(coffee2));
    }

    @Test
    public void getAllCoffees() {
        StepVerifier.create(service.getAllCoffees())
                .expectNext(coffee1)
                .expectNext(coffee2)
                .verifyComplete();
    }

    @Test
    public void getCoffeeById() {
        StepVerifier.create(service.getCoffeeById(coffee1.getId()))
                .expectNext(coffee1)
                .verifyComplete();
    }

    @Test
    public void getOrdersForCoffee() {
        StepVerifier.withVirtualTime(() -> service.getOrdersForCoffee(coffee2.getId()).take(10))
                .thenAwait(Duration.ofHours(1))
                .expectNextCount(10)
                .verifyComplete();
    }
}