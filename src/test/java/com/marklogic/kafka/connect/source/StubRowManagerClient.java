/*
 * Copyright (c) 2019-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 */
package com.marklogic.kafka.connect.source;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.row.RowManager;

import java.lang.reflect.Proxy;
import java.util.function.Consumer;

/**
 * Allows the PlanInvoker implementations to be unit tested without a MarkLogic connection by intercepting
 * {@code resultDoc} and populating the handle that the invoker passed in.
 */
final class StubRowManagerClient {

    static DatabaseClient returningResultDoc(Consumer<Object> populateHandle) {
        RowManager rowManager = (RowManager) Proxy.newProxyInstance(RowManager.class.getClassLoader(),
            new Class<?>[]{RowManager.class}, (proxy, method, args) -> {
                if ("resultDoc".equals(method.getName())) {
                    populateHandle.accept(args[1]);
                    return args[1];
                }
                if (void.class.equals(method.getReturnType())) {
                    return null;
                }
                throw new UnsupportedOperationException(method.getName());
            });

        return (DatabaseClient) Proxy.newProxyInstance(DatabaseClient.class.getClassLoader(),
            new Class<?>[]{DatabaseClient.class}, (proxy, method, args) -> {
                if ("newRowManager".equals(method.getName())) {
                    return rowManager;
                }
                throw new UnsupportedOperationException(method.getName());
            });
    }

    private StubRowManagerClient() {
    }
}
