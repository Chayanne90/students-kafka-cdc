package com.cpdevelops.kafka_cdc.students_service.controller;

import com.cpdevelops.kafka_cdc.students_service.dto.responses.ReplayResult;
import com.cpdevelops.kafka_cdc.students_service.service.DeadLetterReplayService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/admin/dead-letters")
public class DeadLetterAdminController {

    private final DeadLetterReplayService replayService;

    public DeadLetterAdminController(DeadLetterReplayService replayService) {
        this.replayService = replayService;
    }

    @PostMapping("/replay")
    public ReplayResult replay() {
        return replayService.replayAll();
    }
}