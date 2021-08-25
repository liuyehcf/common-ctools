// This file is made available under Elastic License v2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/util/path_util_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "promise.h"

#include <gtest/gtest.h>

#include <thread>

namespace async {

    TEST(PromiseTest, StatusTest) {
        {
            Promise<std::nullptr_t> promise;
            ASSERT_FALSE(promise.is_canceled());
            ASSERT_FALSE(promise.is_done());
            ASSERT_FALSE(promise.is_success());
            ASSERT_FALSE(promise.is_failure());
        }

        {
            Promise<std::nullptr_t> promise;

            promise.try_cancel();

            ASSERT_TRUE(promise.is_canceled());
            ASSERT_TRUE(promise.is_done());
            ASSERT_FALSE(promise.is_success());
            ASSERT_TRUE(promise.is_failure());
        }

        {
            Promise<std::nullptr_t> promise;

            promise.try_success(std::make_shared<std::nullptr_t>());

            ASSERT_FALSE(promise.is_canceled());
            ASSERT_TRUE(promise.is_done());
            ASSERT_TRUE(promise.is_success());
            ASSERT_FALSE(promise.is_failure());
        }

        {
            Promise<std::nullptr_t> promise;

            promise.try_failure("unknown error");

            ASSERT_FALSE(promise.is_canceled());
            ASSERT_TRUE(promise.is_done());
            ASSERT_FALSE(promise.is_success());
            ASSERT_TRUE(promise.is_failure());

            ASSERT_EQ("unknown error", promise.cause());
        }
    }

    TEST(PromiseTest, TestIdempotent) {
        {
            Promise<std::nullptr_t> promise;
            ASSERT_TRUE(promise.try_cancel());
            ASSERT_FALSE(promise.try_cancel());

            ASSERT_FALSE(promise.try_success(nullptr));
            ASSERT_FALSE(promise.try_success(nullptr));

            ASSERT_FALSE(promise.try_failure("something"));
            ASSERT_FALSE(promise.try_failure("something"));
        }

        {
            Promise<std::nullptr_t> promise;
            ASSERT_TRUE(promise.try_success(nullptr));
            ASSERT_FALSE(promise.try_success(nullptr));

            ASSERT_FALSE(promise.try_cancel());
            ASSERT_FALSE(promise.try_cancel());

            ASSERT_FALSE(promise.try_failure("something"));
            ASSERT_FALSE(promise.try_failure("something"));
        }

        {
            Promise<std::nullptr_t> promise;
            ASSERT_TRUE(promise.try_failure("something"));
            ASSERT_FALSE(promise.try_failure("something"));

            ASSERT_FALSE(promise.try_success(nullptr));
            ASSERT_FALSE(promise.try_success(nullptr));

            ASSERT_FALSE(promise.try_cancel());
            ASSERT_FALSE(promise.try_cancel());
        }
    }

    TEST(PromiseTest, TestGet) {
        {
            Promise<int> promise;

            promise.try_success(std::make_shared<int>(5));
            PromiseOutcome<int> outcome = promise.get();
            ASSERT_TRUE(outcome.ok());
            ASSERT_EQ(5, *outcome.outcome());
            ASSERT_EQ("ok", outcome.message());
        }

        {
            Promise<int> promise;

            promise.try_success(nullptr);
            PromiseOutcome<int> outcome = promise.get();
            ASSERT_TRUE(outcome.ok());
            ASSERT_EQ(nullptr, outcome.outcome());
            ASSERT_EQ("ok", outcome.message());
        }

        {
            Promise<int> promise;

            promise.try_cancel();
            PromiseOutcome<int> outcome = promise.get();
            ASSERT_FALSE(outcome.ok());
            ASSERT_EQ(nullptr, outcome.outcome());
            ASSERT_EQ("canceled", outcome.message());
        }

        {
            Promise<int> promise;

            promise.try_failure("something wrong");
            PromiseOutcome<int> outcome = promise.get();
            ASSERT_FALSE(outcome.ok());
            ASSERT_EQ(nullptr, outcome.outcome());
            ASSERT_EQ("something wrong", outcome.message());
        }
    }

    TEST(PromiseTest, TestGetTimeout) {
        {
            Promise<int> promise;

            PromiseOutcome<int> outcome = promise.get(std::chrono::nanoseconds(1000000));
            ASSERT_FALSE(outcome.ok());
            ASSERT_EQ(nullptr, outcome.outcome());
            ASSERT_EQ("timeout", outcome.message());
        }

        {
            Promise<int> promise;

            promise.try_success(std::make_shared<int>(5));
            PromiseOutcome<int> outcome = promise.get(std::chrono::nanoseconds(1000000));
            ASSERT_TRUE(outcome.ok());
            ASSERT_EQ(5, *outcome.outcome());
            ASSERT_EQ("ok", outcome.message());
        }

        {
            Promise<int> promise;

            promise.try_success(nullptr);
            PromiseOutcome<int> outcome = promise.get(std::chrono::nanoseconds(1000000));
            ASSERT_TRUE(outcome.ok());
            ASSERT_EQ(nullptr, outcome.outcome());
            ASSERT_EQ("ok", outcome.message());
        }

        {
            Promise<int> promise;

            promise.try_cancel();
            PromiseOutcome<int> outcome = promise.get(std::chrono::nanoseconds(1000000));
            ASSERT_FALSE(outcome.ok());
            ASSERT_EQ(nullptr, outcome.outcome());
            ASSERT_EQ("canceled", outcome.message());
        }

        {
            Promise<int> promise;

            promise.try_failure("something wrong");
            PromiseOutcome<int> outcome = promise.get(std::chrono::nanoseconds(1000000));
            ASSERT_FALSE(outcome.ok());
            ASSERT_EQ(nullptr, outcome.outcome());
            ASSERT_EQ("something wrong", outcome.message());
        }
    }

    TEST(PromiseTest, TestTryCancelCompetition) {
        int test_num = 100;
        int thread_num = 20;

        for (int i = 0; i < test_num; i++) {
            Promise<int> promise;
            std::mutex lock;
            int count = 0;

            std::function<void()> func = [&]() {
                if (promise.try_cancel()) {
                    std::lock_guard<std::mutex> l(lock);
                    count++;
                }
            };

            std::vector<std::thread> threads;
            threads.reserve(thread_num);
            for (int j = 0; j < thread_num; j++) {
                threads.emplace_back(func);
            }

            for (int j = 0; j < thread_num; j++) {
                threads[j].join();
            }

            ASSERT_EQ(1, count);
        }
    }

    TEST(PromiseTest, TestTrySuccessCompetition) {
        int test_num = 100;
        int thread_num = 20;

        for (int i = 0; i < test_num; i++) {
            Promise<int> promise;
            std::mutex lock;
            int count = 0;

            std::function<void()> func = [&]() {
                if (promise.try_success(std::make_shared<int>(5))) {
                    std::lock_guard<std::mutex> l(lock);
                    count++;
                }
            };

            std::vector<std::thread> threads;
            threads.reserve(thread_num);
            for (int j = 0; j < thread_num; j++) {
                threads.emplace_back(func);
            }

            for (int j = 0; j < thread_num; j++) {
                threads[j].join();
            }

            ASSERT_EQ(1, count);
        }
    }

    TEST(PromiseTest, TestTryFailureCompetition) {
        int test_num = 100;
        int thread_num = 20;

        for (int i = 0; i < test_num; i++) {
            Promise<int> promise;
            std::mutex lock;
            int count = 0;

            std::function<void()> func = [&]() {
                if (promise.try_failure("something wrong")) {
                    std::lock_guard<std::mutex> l(lock);
                    count++;
                }
            };

            std::vector<std::thread> threads;
            threads.reserve(thread_num);
            for (int j = 0; j < thread_num; j++) {
                threads.emplace_back(func);
            }

            for (int j = 0; j < thread_num; j++) {
                threads[j].join();
            }

            ASSERT_EQ(1, count);
        }
    }

    TEST(PromiseTest, TestMixedCompetition) {
        int test_num = 100;
        int thread_num = 20;

        for (int i = 0; i < test_num; i++) {
            Promise<int> promise;
            std::mutex lock;
            int count = 0;

            std::function<void()> funcs[3];

            funcs[0] = [&]() {
                if (promise.try_cancel()) {
                    std::lock_guard<std::mutex> l(lock);
                    count++;
                }
            };

            funcs[1] = [&]() {
                if (promise.try_success(std::make_shared<int>(6))) {
                    std::lock_guard<std::mutex> l(lock);
                    count++;
                }
            };

            funcs[2] = [&]() {
                if (promise.try_failure("something wrong")) {
                    std::lock_guard<std::mutex> l(lock);
                    count++;
                }
            };

            std::vector<std::thread> threads;
            threads.reserve(thread_num);
            for (int j = 0; j < thread_num; j++) {
                threads.emplace_back(funcs[j % 3]);
            }

            for (int j = 0; j < thread_num; j++) {
                threads[j].join();
            }

            ASSERT_EQ(1, count);
        }
    }

    TEST(PromiseTest, TestBlockAndNotifyByCancel) {
        int test_num = 100;
        int thread_num = 20;

        for (int i = 0; i < test_num; i++) {
            Promise<int> promise;
            std::mutex lock;
            int count = 0;

            std::function<void()> func = [&]() {
                PromiseOutcome<int> outcome = promise.get();
                if (!outcome.ok() && outcome.message() == "canceled") {
                    std::lock_guard<std::mutex> l(lock);
                    count++;
                }
            };

            std::vector<std::thread> threads;
            threads.reserve(thread_num);
            for (int j = 0; j < thread_num; j++) {
                threads.emplace_back(func);
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1));

            promise.try_cancel();

            for (int j = 0; j < thread_num; j++) {
                threads[j].join();
            }

            ASSERT_EQ(thread_num, count);
        }
    }

    TEST(PromiseTest, TestBlockAndNotifyBySuccess) {
        int test_num = 100;
        int thread_num = 20;
        int expected_value = 5;

        for (int i = 0; i < test_num; i++) {
            Promise<int> promise;
            std::mutex lock;
            int count = 0;

            std::function<void()> func = [&]() {
                PromiseOutcome<int> outcome = promise.get();
                if (outcome.ok() && *outcome.outcome() == expected_value) {
                    std::lock_guard<std::mutex> l(lock);
                    count++;
                }
            };

            std::vector<std::thread> threads;
            threads.reserve(thread_num);
            for (int j = 0; j < thread_num; j++) {
                threads.emplace_back(func);
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1));

            promise.try_success(std::make_shared<int>(expected_value));

            for (int j = 0; j < thread_num; j++) {
                threads[j].join();
            }

            ASSERT_EQ(thread_num, count);
        }
    }

    TEST(PromiseTest, TestBlockAndNotifyByFailure) {
        int test_num = 100;
        int thread_num = 20;

        for (int i = 0; i < test_num; i++) {
            Promise<int> promise;
            std::mutex lock;
            int count = 0;

            std::function<void()> func = [&]() {
                PromiseOutcome<int> outcome = promise.get();
                if (!outcome.ok() && outcome.message() == "something wrong") {
                    std::lock_guard<std::mutex> l(lock);
                    count++;
                }
            };

            std::vector<std::thread> threads;
            threads.reserve(thread_num);
            for (int j = 0; j < thread_num; j++) {
                threads.emplace_back(func);
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1));

            promise.try_failure("something wrong");

            for (int j = 0; j < thread_num; j++) {
                threads[j].join();
            }

            ASSERT_EQ(thread_num, count);
        }
    }

    TEST(PromiseTest, TestBlockAndTimeout) {
        int test_num = 100;
        int thread_num = 20;

        for (int i = 0; i < test_num; i++) {
            Promise<int> promise;
            std::mutex lock;
            int count = 0;

            std::function<void()> func = [&]() {
                PromiseOutcome<int> outcome = promise.get(std::chrono::nanoseconds(1000000));
                if (!outcome.ok() && outcome.message() == "timeout") {
                    std::lock_guard<std::mutex> l(lock);
                    count++;
                }
            };

            std::vector<std::thread> threads;
            threads.reserve(thread_num);
            for (int j = 0; j < thread_num; j++) {
                threads.emplace_back(func);
            }

            for (int j = 0; j < thread_num; j++) {
                threads[j].join();
            }

            ASSERT_EQ(thread_num, count);
        }
    }

    TEST(PromiseTest, TestListenerAddedBeforeCancel) {
        int test_num = 100;

        for (int i = 0; i < test_num; i++) {
            Promise<int> promise;
            std::mutex lock;
            int count = 0;

            promise.add_listener(std::make_shared<Promise<int>::PromiseListener>([&](Promise<int> &p) {
                if (p.is_canceled()) {
                    std::lock_guard<std::mutex> l(lock);
                    count++;
                }
            }));

            std::function<void()> func = [&]() {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));

                promise.try_cancel();
                promise.try_cancel();
                promise.try_cancel();
            };

            std::thread thread = std::thread(func);
            thread.join();

            ASSERT_EQ(1, count);
        }
    }

    TEST(PromiseTest, TestListenerAddedAfterCancel) {
        int test_num = 100;
        int listener_num = 20;

        for (int i = 0; i < test_num; i++) {
            Promise<int> promise;
            std::mutex lock;
            int count = 0;

            promise.try_cancel();

            for (int j = 0; j < listener_num; j++) {
                promise.add_listener(
                        std::make_shared<Promise<int>::PromiseListener>([&](Promise<int> &p) {
                            if (p.is_canceled()) {
                                std::lock_guard<std::mutex> l(lock);
                                count++;
                            }
                        }));
            }

            ASSERT_EQ(listener_num, count);
        }
    }

    TEST(PromiseTest, TestListenerAddedBeforeSuccess) {
        int test_num = 100;
        int expected_value = 5;

        for (int i = 0; i < test_num; i++) {
            Promise<int> promise;
            std::mutex lock;
            int count = 0;

            promise.add_listener(std::make_shared<Promise<int>::PromiseListener>([&](Promise<int> &p) {
                if (p.is_success() && p.get().ok() && *p.get().outcome() == expected_value) {
                    std::lock_guard<std::mutex> l(lock);
                    count++;
                }
            }));

            std::function<void()> func = [&]() {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));

                promise.try_success(std::make_shared<int>(expected_value));
                promise.try_success(std::make_shared<int>(expected_value));
                promise.try_success(std::make_shared<int>(expected_value));
            };

            std::thread thread = std::thread(func);
            thread.join();

            ASSERT_EQ(1, count);
        }
    }

    TEST(PromiseTest, TestListenerAddedAfterSuccess) {
        int test_num = 100;
        int expected_value = 5;
        int listener_num = 20;

        for (int i = 0; i < test_num; i++) {
            Promise<int> promise;
            std::mutex lock;
            int count = 0;

            promise.try_success(std::make_shared<int>(expected_value));

            for (int j = 0; j < listener_num; j++) {
                promise.add_listener(
                        std::make_shared<Promise<int>::PromiseListener>([&](Promise<int> &p) {
                            if (p.is_success() && p.get().ok() &&
                                *p.get().outcome() == expected_value) {
                                std::lock_guard<std::mutex> l(lock);
                                count++;
                            }
                        }));
            }

            ASSERT_EQ(listener_num, count);
        }
    }

    TEST(PromiseTest, TestListenerAddedBeforeFailure) {
        int test_num = 100;

        for (int i = 0; i < test_num; i++) {
            Promise<int> promise;
            std::mutex lock;
            int count = 0;

            promise.add_listener(std::make_shared<Promise<int>::PromiseListener>([&](Promise<int> &p) {
                if (p.is_failure() && p.cause() == "something wrong") {
                    std::lock_guard<std::mutex> l(lock);
                    count++;
                }
            }));

            std::function<void()> func = [&]() {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));

                promise.try_failure("something wrong");
                promise.try_failure("something wrong");
                promise.try_failure("something wrong");
            };

            std::thread thread = std::thread(func);
            thread.join();

            ASSERT_EQ(1, count);
        }
    }

    TEST(PromiseTest, TestListenerAddedAfterFailure) {
        int test_num = 100;
        int listener_num = 20;

        for (int i = 0; i < test_num; i++) {
            Promise<int> promise;
            std::mutex lock;
            int count = 0;

            promise.try_failure("something wrong");

            for (int j = 0; j < listener_num; j++) {
                promise.add_listener(
                        std::make_shared<Promise<int>::PromiseListener>([&](Promise<int> &p) {
                            if (p.is_failure() && p.cause() == "something wrong") {
                                std::lock_guard<std::mutex> l(lock);
                                count++;
                            }
                        }));
            }

            ASSERT_EQ(listener_num, count);
        }
    }

} // namespace async
