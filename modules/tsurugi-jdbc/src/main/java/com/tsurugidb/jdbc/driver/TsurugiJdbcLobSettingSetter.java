/*
 * Copyright 2025-2026 Project Tsurugi.
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
package com.tsurugidb.jdbc.driver;

import java.net.URI;
import java.util.List;

import com.tsurugidb.jdbc.TsurugiJdbcLobTransferType;

/**
 * Tsurugi JDBC Large Object Setting Setter.
 *
 * @since 0.5.0
 */
public interface TsurugiJdbcLobSettingSetter {

    /**
     * Set large object transfer type.
     *
     * @param lobTransferType large object transfer type
     */
    public void setLobTransferType(TsurugiJdbcLobTransferType lobTransferType);

    /**
     * Set large object path mapping on send.
     * <p>
     * The format of path mapping is "client-path:server-path".
     * </p>
     *
     * @param pathMapping large object path mapping on send
     */
    public void setLobPathMappingOnSend(List<String> pathMapping);

    /**
     * Set large object path mapping on receive.
     * <p>
     * The format of path mapping is "client-path:server-path".
     * </p>
     *
     * @param pathMapping large object path mapping on receive
     */
    public void setLobPathMappingOnReceive(List<String> pathMapping);

    /**
     * Set blob relay service endpoint.
     *
     * @param endpoint blob relay service endpoint
     */
    public void setBlobRelayServiceEndpoint(URI endpoint);
}
