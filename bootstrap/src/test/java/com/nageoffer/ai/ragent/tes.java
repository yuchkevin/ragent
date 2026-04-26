package com.nageoffer.ai.ragent;


import com.google.common.util.concurrent.*;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class tes {

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(5);
        ListeningExecutorService pool = MoreExecutors.listeningDecorator(executor);

        // -------------- 开始地狱 --------------

        ListenableFuture<String> future1 = pool.submit(() -> "任务1");

        Futures.addCallback(future1, new FutureCallback<String>() {
            @Override
            public void onSuccess(String res1) {

                // 嵌套第一层
                ListenableFuture<String> future2 = pool.submit(() -> res1 + " → 任务2");

                Futures.addCallback(future2, new FutureCallback<String>() {
                    @Override
                    public void onSuccess(String res2) {

                        // 嵌套第二层
                        ListenableFuture<String> future3 = pool.submit(() -> res2 + " → 任务3");

                        Futures.addCallback(future3, new FutureCallback<String>() {
                            @Override
                            public void onSuccess(String res3) {
                                System.out.println(res3);
                            }

                            @Override
                            public void onFailure(Throwable t) {}
                        }, pool);
                    }

                    @Override
                    public void onFailure(Throwable t) {}
                }, pool);
            }

            @Override
            public void onFailure(Throwable t) {}
        }, pool);
    }
}
