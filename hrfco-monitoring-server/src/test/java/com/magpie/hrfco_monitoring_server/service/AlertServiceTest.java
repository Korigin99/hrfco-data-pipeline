package com.magpie.hrfco_monitoring_server.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import static org.junit.jupiter.api.Assertions.*;

class AlertServiceTest {

    private AlertService alertService;

    @BeforeEach
    void setUp() {
        alertService = new AlertService();
    }

    @Test
    void subscribe_returnsEmitter() {
        SseEmitter emitter = alertService.subscribe(null);
        assertNotNull(emitter);
    }

    @Test
    void subscribe_withStation_returnsEmitter() {
        SseEmitter emitter = alertService.subscribe("1001602");
        assertNotNull(emitter);
    }

    @Test
    void getSubscriberCount_initiallyZeroOrMore() {
        // subscribe creates emitters, so after subscribe count should increase
        int before = alertService.getSubscriberCount();
        alertService.subscribe(null);
        int after = alertService.getSubscriberCount();
        assertTrue(after >= before + 1);
    }

    @Test
    void getSubscriberCount_multipleSubscribers() {
        alertService.subscribe(null);
        alertService.subscribe("1001602");
        alertService.subscribe("1001603");

        assertTrue(alertService.getSubscriberCount() >= 3);
    }

    @Test
    void broadcastAlert_doesNotThrowWithNoSubscribers() {
        assertDoesNotThrow(() ->
            alertService.broadcastAlert("1001602", "{\"test\":true}"));
    }

    @Test
    void broadcastAlert_withSubscribers_doesNotThrow() {
        alertService.subscribe(null);
        alertService.subscribe("1001602");

        assertDoesNotThrow(() ->
            alertService.broadcastAlert("1001602", "{\"warningLevel\":\"NORMAL\"}"));
    }

    @Test
    void broadcastAlert_withNullStation_doesNotThrow() {
        alertService.subscribe(null);

        assertDoesNotThrow(() ->
            alertService.broadcastAlert(null, "{\"test\":true}"));
    }
}
