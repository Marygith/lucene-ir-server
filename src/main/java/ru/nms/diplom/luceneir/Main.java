package ru.nms.diplom.luceneir;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import ru.nms.diplom.luceneir.service.SearchServiceImpl;

import java.io.IOException;

public class Main {

    public static void main(String[] args) {

        Server server = ServerBuilder
                .forPort(8080)
                .addService(new SearchServiceImpl()).build();

        try {
            server.start();
            server.awaitTermination();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("did not manage to start server", e);
        }
    }

}
