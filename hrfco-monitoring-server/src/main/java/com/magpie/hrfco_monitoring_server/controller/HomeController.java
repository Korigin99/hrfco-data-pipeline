package com.magpie.hrfco_monitoring_server.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * 홈 컨트롤러 - 모니터링 UI 제공
 */
@Controller
public class HomeController {
    
    /**
     * 루트 경로로 접근 시 모니터링 페이지 반환
     */
    @GetMapping("/")
    public String home() {
        return "test.html";
    }
    
    /**
     * 명시적 모니터링 페이지 경로
     */
    @GetMapping("/monitoring")
    public String monitoring() {
        return "test.html";
    }
}

