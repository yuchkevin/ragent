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

package com.nageoffer.ai.ragent.framework.mq.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ 配置类
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class RabbitMQConfig {

    // ==================== 消息反馈队列 ====================
    @Bean
    public DirectExchange messageFeedbackExchange(
            @Value("${rabbitmq.message-feedback.exchange}") String exchange) {
        return new DirectExchange(exchange);
    }

    @Bean
    public Queue messageFeedbackQueue(
            @Value("${rabbitmq.message-feedback.queue}") String queue,
            @Value("${rabbitmq.message-feedback.dead-queue}") String dlq) {
        return QueueBuilder.durable(queue)
                .withArgument("x-dead-letter-exchange", "${rabbitmq.message-feedback.dead-exchange}")
                .withArgument("x-dead-letter-routing-key", dlq)
                .build();
    }

    @Bean
    public Binding messageFeedbackBinding(
            Queue messageFeedbackQueue,
            DirectExchange messageFeedbackExchange,
            @Value("${rabbitmq.message-feedback.routing-key}") String routingKey) {
        return BindingBuilder.bind(messageFeedbackQueue).to(messageFeedbackExchange).with(routingKey);
    }

    @Bean
    public DirectExchange messageFeedbackDeadExchange(
            @Value("${rabbitmq.message-feedback.dead-exchange}") String dlx) {
        return new DirectExchange(dlx);
    }

    @Bean
    public Queue messageFeedbackDeadQueue(
            @Value("${rabbitmq.message-feedback.dead-queue}") String dlq) {
        return QueueBuilder.durable(dlq).build();
    }

    @Bean
    public Binding messageFeedbackDeadBinding(
            Queue messageFeedbackDeadQueue,
            DirectExchange messageFeedbackDeadExchange,
            @Value("${rabbitmq.message-feedback.dead-queue}") String routingKey) {
        return BindingBuilder.bind(messageFeedbackDeadQueue).to(messageFeedbackDeadExchange).with(routingKey);
    }

    // ==================== 文档分块队列 ====================
    @Bean
    public DirectExchange knowledgeDocumentChunkExchange(
            @Value("${rabbitmq.knowledge-document-chunk.exchange}") String exchange) {
        return new DirectExchange(exchange);
    }

    @Bean
    public Queue knowledgeDocumentChunkQueue(
            @Value("${rabbitmq.knowledge-document-chunk.queue}") String queue,
            @Value("${rabbitmq.knowledge-document-chunk.dead-queue}") String dlq) {
        return QueueBuilder.durable(queue)
                .withArgument("x-dead-letter-exchange", "${rabbitmq.knowledge-document-chunk.dead-exchange}")
                .withArgument("x-dead-letter-routing-key", dlq)
                .build();
    }

    @Bean
    public Binding knowledgeDocumentChunkBinding(
            Queue knowledgeDocumentChunkQueue,
            DirectExchange knowledgeDocumentChunkExchange,
            @Value("${rabbitmq.knowledge-document-chunk.routing-key}") String routingKey) {
        return BindingBuilder.bind(knowledgeDocumentChunkQueue).to(knowledgeDocumentChunkExchange).with(routingKey);
    }

    @Bean
    public DirectExchange knowledgeDocumentChunkDeadExchange(
            @Value("${rabbitmq.knowledge-document-chunk.dead-exchange}") String dlx) {
        return new DirectExchange(dlx);
    }

    @Bean
    public Queue knowledgeDocumentChunkDeadQueue(
            @Value("${rabbitmq.knowledge-document-chunk.dead-queue}") String dlq) {
        return QueueBuilder.durable(dlq).build();
    }

    @Bean
    public Binding knowledgeDocumentChunkDeadBinding(
            Queue knowledgeDocumentChunkDeadQueue,
            DirectExchange knowledgeDocumentChunkDeadExchange,
            @Value("${rabbitmq.knowledge-document-chunk.dead-queue}") String routingKey) {
        return BindingBuilder.bind(knowledgeDocumentChunkDeadQueue).to(knowledgeDocumentChunkDeadExchange).with(routingKey);
    }

    // ==================== RabbitTemplate 配置 ====================
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(new Jackson2JsonMessageConverter());
        template.setConfirmCallback((correlationData, ack, cause) -> {
            if (!ack) {
                log.error("消息发送失败: correlationData={}, cause={}", correlationData, cause);
            }
        });
        template.setReturnsCallback(returned -> {
            log.error("消息返回: exchange={}, routingKey={}, replyCode={}, replyText={}",
                    returned.getExchange(), returned.getRoutingKey(),
                    returned.getReplyCode(), returned.getReplyText());
        });
        return template;
    }

    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(new Jackson2JsonMessageConverter());
        factory.setDefaultRequeueRejected(false);
        return factory;
    }
}