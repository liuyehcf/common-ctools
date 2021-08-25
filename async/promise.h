// This file is made available under Elastic License v2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/threadpool.h

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

#ifndef DORIS_PROMISE_H
#define DORIS_PROMISE_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace async {

    template<typename T>
    class PromiseListenerWrapper;

    template<typename T>
    class Promise;

    template<typename T>
    class PromiseOutcome {
        friend class Promise<T>;

    public:
        // Return true only if promise is success
        // Other possible situations include cancel/failure/timout
        bool ok() { return _ok; }

        std::shared_ptr<T> outcome() { return _outcome; }

        const std::string &message() { return _message; }

    private:
        PromiseOutcome(bool ok, std::shared_ptr<T> outcome, std::string message)
                : _ok(ok), _outcome(outcome), _message(std::move(message)) {}

        bool _ok;
        std::shared_ptr<T> _outcome;
        std::string _message;
    };

    template<typename T>
    class Promise {
    public:
        using PromiseListener = std::function<void(Promise<T> &)>;

        // Return true if this promise was canceled before it completed normally
        // Otherwise return false
        bool is_canceled() { return _is_canceled; }

        // Return true if this promise was finished
        // including success, failure, cancel
        // Otherwise return false
        bool is_done() { return _is_done; }

        // Return true if this promise was succeeded
        // Otherwise return false
        bool is_success() { return _is_success; }

        // Return true if this promise was failed
        // Otherwise return false
        bool is_failure() { return _is_failure; }

        // Get cause of this promise
        // The method is valid only if is_done() returns true and is_success() return false
        // Otherwise undefined
        std::string cause() { return _cause; }

        // Try to mark this promise as canceled, and there are only one of these method calls can succeed,
        // including try_success/try_failure/try_cancel
        // this method will trigger all listeners if necessary
        bool try_cancel();

        // Try to mark this promise as succeeded, and there are only one of these method calls can succeed,
        // including try_success/try_failure/try_cancel
        // this method will trigger all listeners if necessary
        bool try_success(std::shared_ptr<T> outcome);

        // Try to mark this promise as failed, and there are only one of these method calls can succeed,
        // including try_success/try_failure/try_cancel
        // this method will trigger all listeners if necessary
        bool try_failure(const std::string &cause);

        // Add listener to this promise
        // Guarantee: no matter when the listener added to the promise, listener will be trigger exactly
        // once as long as promise finished(success/failure/cancel all means finished)
        Promise<T> &add_listener(std::shared_ptr<PromiseListener> listener);

        // Waits promise to be done, including three cases success/failure/cancel
        // 1. PromiseOutcome.ok() == true means promise is success, and you can fetch outcome by calling
        // PromiseOutcome.outcome(). Otherwise, result of PromiseOutcome.outcome() is undefined
        // 2. PromiseOutcome.ok() == false means promise is canceled or failed, and you can fetch error message
        // from PromiseOutcome.message()
        PromiseOutcome<T> get();

        // Waits promise to be done at most the given timeout, including three cases success/failure/cancel
        // 1. PromiseOutcome.ok() == true means promise is success, and you can fetch outcome by calling
        // PromiseOutcome.outcome(). Otherwise, result of PromiseOutcome.outcome() is undefined
        // 2. PromiseOutcome.ok() == false means promise is canceled or failed or timeout, and you can fetch
        // error message from PromiseOutcome.message()
        PromiseOutcome<T> get(const std::chrono::nanoseconds &timeout);

    private:
        volatile bool _is_canceled = false;
        volatile bool _is_done = false;
        volatile bool _is_success = false;
        volatile bool _is_failure = false;
        std::string _cause;
        std::shared_ptr<T> _outcome;

        std::vector<std::shared_ptr<PromiseListenerWrapper<T>>> _listeners;
        std::mutex _listener_lock;
        std::mutex _main_lock;
        std::condition_variable _complete_cv;

        bool execute_synchronous_under_lock_for_bool(
                const std::function<bool(std::unique_lock<std::mutex> &)> &callable);

        PromiseOutcome<T> execute_synchronous_under_lock_for_outcome(
                const std::function<PromiseOutcome<T>(std::unique_lock<std::mutex> &)> &callable);

        void add_listener0(std::shared_ptr<PromiseListener> listener);

        void notify_all_listeners();

        void set_canceled_under_lock();

        void set_success_under_lock(std::shared_ptr<T> outcome);

        void set_failure_under_lock(const std::string &cause);

        PromiseOutcome<T> get_outcome(bool is_timeout);
    };

    template<typename T>
    class PromiseListenerWrapper {
        friend class Promise<T>;

    private:
        using PromiseListener = std::function<void(Promise<T> &)>;

        std::shared_ptr<PromiseListener> _target;
        bool _is_triggerred = false;
        std::mutex _lock;

        void operator()(Promise<T> &promise) {
            if (_is_triggerred) {
                return;
            }

            // guarantee _target only trigger once
            std::unique_lock<std::mutex> l(_lock);
            if (_is_triggerred) {
                return;
            }
            _is_triggerred = true;
            _target->operator()(promise);
        }

    public:
        explicit PromiseListenerWrapper(std::shared_ptr<PromiseListener> target) : _target(target) {}
    };

    template<typename T>
    bool Promise<T>::try_cancel() {
        if (is_done()) {
            return false;
        }

        bool result = execute_synchronous_under_lock_for_bool(
                [this](std::unique_lock<std::mutex> &l) -> bool {
                    if (is_done()) {
                        return false;
                    }

                    set_canceled_under_lock();

                    return true;
                });

        notify_all_listeners();

        return result;
    }

    template<typename T>
    bool Promise<T>::try_success(std::shared_ptr<T> outcome) {
        if (is_done()) {
            return false;
        }

        bool result = execute_synchronous_under_lock_for_bool(
                [this, outcome](std::unique_lock<std::mutex> &l) -> bool {
                    if (is_done()) {
                        return false;
                    }

                    set_success_under_lock(outcome);

                    return true;
                });

        notify_all_listeners();

        return result;
    }

    template<typename T>
    bool Promise<T>::try_failure(const std::string &cause) {
        if (is_done()) {
            return false;
        }

        bool result = execute_synchronous_under_lock_for_bool(
                [this, cause](std::unique_lock<std::mutex> &l) -> bool {
                    if (is_done()) {
                        return false;
                    }

                    set_failure_under_lock(cause);

                    return true;
                });

        notify_all_listeners();

        return result;
    }

    template<typename T>
    Promise<T> &Promise<T>::add_listener(std::shared_ptr<PromiseListener> listener) {
        add_listener0(listener);

        if (is_done()) {
            notify_all_listeners();
        }

        return *this;
    }

    template<typename T>
    PromiseOutcome<T> Promise<T>::get() {
        if (is_done()) {
            return get_outcome(false);
        }

        return execute_synchronous_under_lock_for_outcome(
                [this](std::unique_lock<std::mutex> &l) -> PromiseOutcome<T> {
                    if (is_done()) {
                        return get_outcome(false);
                    }

                    _complete_cv.wait(l);
                    return get_outcome(false);
                });
    }

    template<typename T>
    PromiseOutcome<T> Promise<T>::get(const std::chrono::nanoseconds &timeout) {
        if (is_done()) {
            return get_outcome(false);
        }

        return execute_synchronous_under_lock_for_outcome(
                [this, &timeout](std::unique_lock<std::mutex> &l) -> PromiseOutcome<T> {
                    if (is_done()) {
                        return get_outcome(false);
                    }

                    if (_complete_cv.template wait_for(l, timeout) == std::cv_status::no_timeout) {
                        return get_outcome(false);
                    } else {
                        return get_outcome(true);
                    }
                });
    }

    template<typename T>
    bool Promise<T>::execute_synchronous_under_lock_for_bool(
            const std::function<bool(std::unique_lock<std::mutex> &)> &callable) {
        {
            std::unique_lock<std::mutex> l(_main_lock);
            return callable(l);
        }
    }

    template<typename T>
    PromiseOutcome<T> Promise<T>::execute_synchronous_under_lock_for_outcome(
            const std::function<PromiseOutcome<T>(std::unique_lock<std::mutex> &)> &callable) {
        {
            std::unique_lock<std::mutex> l(_main_lock);
            return callable(l);
        }
    }

    template<typename T>
    void Promise<T>::add_listener0(std::shared_ptr<PromiseListener> listener) {
        {
            std::lock_guard<std::mutex> l(_listener_lock);
            _listeners.push_back(std::make_shared<PromiseListenerWrapper<T>>(listener));
        }
    }

    template<typename T>
    void Promise<T>::notify_all_listeners() {
        for (std::shared_ptr<PromiseListenerWrapper<T>> listener : _listeners) {
            try {
                listener->operator()(*this);
            } catch (std::exception &ignored) {
                // TODO(hcf) log here
            }
        }
    }

    template<typename T>
    void Promise<T>::set_canceled_under_lock() {
        // isDone is used to determine success, so the assignment of isDone must be at the end
        _is_canceled = true;
        _is_done = true;
        _is_failure = true;
        _cause = "canceled";
        _complete_cv.notify_all();
    }

    template<typename T>
    void Promise<T>::set_success_under_lock(std::shared_ptr<T> outcome) {
        // isDone is used to determine success, so the assignment of isDone must be at the end
        _is_done = true;
        _is_success = true;
        _outcome = outcome;
        _complete_cv.notify_all();
    }

    template<typename T>
    void Promise<T>::set_failure_under_lock(const std::string &cause) {
        // isDone is used to determine success, so the assignment of isDone must be at the end
        _is_done = true;
        _is_failure = true;
        _cause = cause;
        _complete_cv.notify_all();
    }

    template<typename T>
    PromiseOutcome<T> Promise<T>::get_outcome(bool is_timeout) {
        if (is_timeout) {
            return {false, nullptr, "timeout"};
        }

        if (!is_done()) {
            // impossible
            return {false, nullptr, "not finished yet"};
        }

        if (is_success()) {
            return {true, _outcome, "ok"};
        }

        if (is_canceled()) {
            return {false, nullptr, "canceled"};
        }

        return {false, nullptr, _cause};
    }
} // namespace ctools

#endif //DORIS_PROMISE_H
