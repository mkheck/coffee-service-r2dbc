package com.thehecklers.coffeeservicer2dbc;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import lombok.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.r2dbc.function.DatabaseClient;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.Instant;

@SpringBootApplication
public class CoffeeServiceR2dbcApplication {

    public static void main(String[] args) {
        SpringApplication.run(CoffeeServiceR2dbcApplication.class, args);
    }
}

@RestController
@RequestMapping("/coffees")
class CoffeeController {
    private final CoffeeService service;

    CoffeeController(CoffeeService service) {
        this.service = service;
    }

    @GetMapping
    Flux<Coffee> all() {
        return service.getAllCoffees();
    }

    @GetMapping("/{id}")
    Mono<Coffee> byId(@PathVariable String id) {
        // MH: This doesn't work currently bc of the type SERIAL in Postgres & String here. Oddly enough, the
        // orders() method below DOES...
        return service.getCoffeeById(id);
    }

    @GetMapping(value = "/{id}/orders", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    Flux<CoffeeOrder> orders(@PathVariable String id) {
        return service.getOrdersForCoffee(id);
    }
}

@Service
class CoffeeService {
    private final CoffeeRepository repo;

    CoffeeService(CoffeeRepository repo) {
        this.repo = repo;
    }

    Flux<Coffee> getAllCoffees() {
        return repo.findAll();
    }

    Mono<Coffee> getCoffeeById(String id) {
        return repo.findById(id);
    }

    Flux<CoffeeOrder> getOrdersForCoffee(String coffeeId) {
        return Flux.<CoffeeOrder>generate(sink -> sink.next(new CoffeeOrder(coffeeId, Instant.now())))
                .delayElements(Duration.ofSeconds(1));
    }
}

@Component
class Demo {
    private final CoffeeRepository repo;

    Demo(CoffeeRepository repo) {
        this.repo = repo;
    }

    @PostConstruct
    private void run() {
        repo.deleteAll().thenMany(
                Flux.just("Blue Bottle Coffee", "Philz Coffee")
                        .map(Coffee::new)
                        .flatMap(repo::save))
                .thenMany(repo.findAll())
                .subscribe(System.out::println);
    }
}

@Configuration
class DbConfig {
    @Bean
    PostgresqlConnectionFactory connectionFactory() {
        return new PostgresqlConnectionFactory(
                PostgresqlConnectionConfiguration.builder() //
                        .host("localhost")
                        .database("postgres")
                        .username("postgres")
                        .password("caffeine")
                        .build());
    }

    @Bean
    DatabaseClient databaseClient(ConnectionFactory factory) {
        return DatabaseClient.builder() //
                .connectionFactory(factory) //
                .build();
    }

    @Bean
    R2dbcRepositoryFactory repositoryFactory(DatabaseClient client) {
        RelationalMappingContext context = new RelationalMappingContext();
        context.afterPropertiesSet();

        return new R2dbcRepositoryFactory(client, context);
    }

    @Bean
    CoffeeRepository coffeeRepository(R2dbcRepositoryFactory factory) {
        return factory.getRepository(CoffeeRepository.class);
    }
}

interface CoffeeRepository extends ReactiveCrudRepository<Coffee, String> {
    @Query("DELETE FROM coffee")
    Mono<Coffee> deleteAllById();
}

@Data
@AllArgsConstructor
class CoffeeOrder {
    private String coffeeId;
    private Instant whenOrdered;
}

@Data
@NoArgsConstructor
@RequiredArgsConstructor
@AllArgsConstructor
class Coffee {
    @Id
    private String id;
    @NonNull
    private String name;
}