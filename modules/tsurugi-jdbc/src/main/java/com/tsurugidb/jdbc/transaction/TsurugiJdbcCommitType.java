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
package com.tsurugidb.jdbc.transaction;

import com.tsurugidb.jdbc.annotation.TsurugiJdbcInternal;
import com.tsurugidb.sql.proto.SqlRequest.CommitStatus;

public enum TsurugiJdbcCommitType {
    /** DEFAULT */
    DEFAULT(CommitStatus.COMMIT_STATUS_UNSPECIFIED),
    /** ACCEPTED */
    ACCEPTED(CommitStatus.ACCEPTED),
    /** AVAILABLE */
    AVAILABLE(CommitStatus.AVAILABLE),
    /** STORED */
    STORED(CommitStatus.STORED),
    /** PROPAGATED */
    PROPAGATED(CommitStatus.PROPAGATED),

    ;

    private final CommitStatus lowType;

    TsurugiJdbcCommitType(CommitStatus lowType) {
        this.lowType = lowType;
    }

    @TsurugiJdbcInternal
    public CommitStatus getLowCommitStatus() {
        return this.lowType;
    }
}
