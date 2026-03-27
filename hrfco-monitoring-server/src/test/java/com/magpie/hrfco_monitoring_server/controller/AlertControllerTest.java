package com.magpie.hrfco_monitoring_server.controller;

import com.magpie.hrfco_monitoring_server.service.AlertService;
import com.magpie.hrfco_monitoring_server.service.NiFiMonitoringService;
import com.magpie.hrfco_monitoring_server.service.StationService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(AlertController.class)
class AlertControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private AlertService alertService;

    @MockitoBean
    private StationService stationService;

    @MockitoBean
    private NiFiMonitoringService nifiMonitoringService;

    @Test
    void health_returnsOK() throws Exception {
        mockMvc.perform(get("/api/alerts/health"))
            .andExpect(status().isOk())
            .andExpect(content().string("OK"));
    }

    @Test
    void getStations_returnsList() throws Exception {
        when(stationService.getAllStations()).thenReturn(
            List.of(Map.of("code", "1001602", "name", "테스트관측소")));

        mockMvc.perform(get("/api/alerts/stations"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$[0].code").value("1001602"))
            .andExpect(jsonPath("$[0].name").value("테스트관측소"));
    }

    @Test
    void systemStatus_returnsAllComponents() throws Exception {
        when(alertService.getSubscriberCount()).thenReturn(5);
        when(nifiMonitoringService.getNiFiStatus()).thenReturn(
            Map.of("status", "ONLINE"));

        mockMvc.perform(get("/api/alerts/system-status"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.alertServer.status").value("ONLINE"))
            .andExpect(jsonPath("$.kafka.status").value("ONLINE"))
            .andExpect(jsonPath("$.kafka.subscribers").value(5))
            .andExpect(jsonPath("$.nifi.status").value("ONLINE"));
    }

    @Test
    void getStationDetail_notFound_returnsError() throws Exception {
        when(stationService.getStationDetail("UNKNOWN")).thenReturn(null);

        mockMvc.perform(get("/api/alerts/station/UNKNOWN"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.error").value("Station not found"));
    }

    @Test
    void getStationDetail_found_returnsDetail() throws Exception {
        when(stationService.getStationDetail("1001602")).thenReturn(
            Map.of("observationCode", "1001602", "obsnm", "테스트"));

        mockMvc.perform(get("/api/alerts/station/1001602"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.observationCode").value("1001602"));
    }
}
