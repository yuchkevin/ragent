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

package com.nageoffer.ai.ragent.framework.mq.producer;

import cn.hutool.core.util.StrUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nageoffer.ai.ragent.framework.mq.MessageWrapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.util.UUID;
import java.util.function.Consumer;

/**
 * 基于 RabbitMQ 的消息生产者
 */
@Slf4j
@RequiredArgsConstructor
public class RabbitMQProducerAdapter implements MessageQueueProducer {

    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;

    @Override
    public SendResult send(String topic, String keys, String bizDesc, Object body) {
        keys = StrUtil.isEmpty(keys) ? UUID.randomUUID().toString() : keys;

        MessageWrapper<Object> wrapper = MessageWrapper.builder()
                .keys(keys)
                .body(body)
                .build();

        String messageJson;
        try {
            messageJson = objectMapper.writeValueAsString(wrapper);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("消息序列化失败", e);
        }

        Message message = MessageBuilder
                .withBody(messageJson.getBytes())
                .setContentType(MessageProperties.CONTENT_TYPE_JSON)
                .setMessageId(keys)
                .setTimestamp(new java.util.Date())
                .build();

        CorrelationData correlationData = new CorrelationData(keys);

        try {
            rabbitTemplate.convertAndSend(topic, topic, message, correlationData);
            log.info("[生产者] {} - 发送成功，消息ID: {}, Keys: {}", bizDesc, keys, keys);
            return new SendResult(keys, SendStatus.SEND_OK);
        } catch (Exception ex) {
            log.error("[生产者] {} - 消息发送失败，topic: {}, keys: {}", bizDesc, topic, keys, ex);
            throw ex;
        }
    }

    @Override
    public void sendInTransaction(String topic, String keys, String bizDesc, Object body,
                                  Consumer<Object> localTransaction) {
        keys = StrUtil.isEmpty(keys) ? UUID.randomUUID().toString() : keys;
        String txId = UUID.randomUUID().toString();

        MessageWrapper<Object> wrapper = MessageWrapper.builder()
                .keys(keys)
                .body(body)
                .build();

        String messageJson;
        try {
            messageJson = objectMapper.writeValueAsString(wrapper);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("消息序列化失败", e);
        }

        Message message = MessageBuilder
                .withBody(messageJson.getBytes())
                .setContentType(MessageProperties.CONTENT_TYPE_JSON)
                .setMessageId(keys)
                .setHeader("tx_id", txId)
                .setTimestamp(new java.util.Date())
                .build();

        // 先执行本地事务
        try {
            localTransaction.accept(null);
            log.info("[生产者] {} - 本地事务执行成功，准备发送消息，txId: {}, Keys: {}", bizDesc, txId, keys);
        } catch (Exception e) {
            log.error("[生产者] {} - 本地事务执行失败，txId: {}, Keys: {}", bizDesc, txId, keys, e);
            throw new RuntimeException("本地事务执行失败", e);
        }

        // 本地事务成功后发送消息
        try {
            CorrelationData correlationData = new CorrelationData(keys);
            rabbitTemplate.convertAndSend(topic, topic, message, correlationData);
            log.info("[生产者] {} - 事务消息发送成功，txId: {}, Keys: {}", bizDesc, txId, keys);
        } catch (Exception ex) {
            log.error("[生产者] {} - 事务消息发送失败，topic: {}, keys: {}", bizDesc, topic, keys, ex);
            throw ex;
        }
    }
}