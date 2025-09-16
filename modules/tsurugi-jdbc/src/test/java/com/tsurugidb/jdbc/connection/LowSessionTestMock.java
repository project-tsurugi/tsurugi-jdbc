/*
 * Copyright 2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.jdbc.connection;

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.Map;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.ShutdownType;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ServerResource;
import com.tsurugidb.tsubakuro.util.Timeout;

public class LowSessionTestMock implements Session {

    private Timeout closeTimeout = new Timeout();
    private final Map<ServerResource, Boolean> resourceSet = new IdentityHashMap<>();

    @Override
    public void connect(Wire sessionWire) {
        throw new UnsupportedOperationException("do override");
    }

    @Override
    public Wire getWire() {
        throw new UnsupportedOperationException("do override");
    }

    @Override
    public void setCloseTimeout(Timeout timeout) {
        this.closeTimeout = timeout;
    }

    @Override
    public Timeout getCloseTimeout() {
        return this.closeTimeout;
    }

    @Override
    public void put(ServerResource resource) {
        resourceSet.put(resource, true);
    }

    @Override
    public void remove(ServerResource resource) {
        resourceSet.remove(resource);
    }

    @Override
    public FutureResponse<Void> shutdown(ShutdownType type) throws IOException {
        return FutureResponse.returns(null);
    }

    @Override
    public void close() throws ServerException, IOException, InterruptedException {
        // do nothing
    }
}
