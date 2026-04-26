/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nageoffer.ai.ragent.knowledge.mq;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.nageoffer.ai.ragent.framework.mq.producer.MessageQueueProducer;
import com.nageoffer.ai.ragent.knowledge.dao.entity.KnowledgeDocumentDO;
import com.nageoffer.ai.ragent.knowledge.dao.mapper.KnowledgeDocumentMapper;
import com.nageoffer.ai.ragent.knowledge.enums.DocumentStatus;
import com.nageoffer.ai.ragent.knowledge.mq.event.KnowledgeDocumentChunkEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 文档分块事务状态补偿器
 * <p>
 * 由于 RabbitMQ 没有内置事务回查机制，通过定时检查数据库状态实现补偿：
 * - 如果文档状态是 RUNNING 但超过 5 分钟未完成，认为本地事务可能丢失
 * - 重新发送消息进行重试
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KnowledgeDocumentChunkCompensator {

    private final KnowledgeDocumentMapper documentMapper;
    private final MessageQueueProducer messageQueueProducer;
    private final RabbitTemplate rabbitTemplate;

    @Value("${rabbitmq.knowledge-document-chunk.exchange}")
    private String exchange;

    @Value("${rabbitmq.knowledge-document-chunk.routing-key}")
    private String routingKey;

    private static final long COMPENSATION_THRESHOLD_MS = 5 * 60 * 1000;

    @Scheduled(fixedDelay = 30000)
    public void checkAndCompensate() {
        List<KnowledgeDocumentDO> runningDocs = documentMapper.selectList(
                Wrappers.lambdaQuery(KnowledgeDocumentDO.class)
                        .eq(KnowledgeDocumentDO::getStatus, DocumentStatus.RUNNING.getCode())
        );

        for (KnowledgeDocumentDO doc : runningDocs) {
            long elapsed = System.currentTimeMillis() - doc.getUpdateTime().getTime();
            if (elapsed > COMPENSATION_THRESHOLD_MS) {
                log.warn("[补偿器] 检测到疑似未完成的任务，docId={}, elapsed={}ms", doc.getId(), elapsed);
                compensate(doc);
            }
        }
    }

    private void compensate(KnowledgeDocumentDO doc) {
        try {
            KnowledgeDocumentChunkEvent event = KnowledgeDocumentChunkEvent.builder()
                    .docId(doc.getId())
                    .kbId(doc.getKbId())
                    .operator(doc.getUpdatedBy())
                    .build();

            rabbitTemplate.convertAndSend(exchange, routingKey, event);
            log.info("[补偿器] 重新发送消息成功，docId={}", doc.getId());
        } catch (Exception e) {
            log.error("[补偿器] 重新发送消息失败，docId={}", doc.getId(), e);
        }
    }
}