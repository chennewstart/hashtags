/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql;

import java.util.EnumSet;

public enum StatementType
{
    SELECT, INSERT, UPDATE, BATCH, USE, TRUNCATE, DELETE, CREATE_KEYSPACE, CREATE_COLUMNFAMILY, CREATE_INDEX, DROP_INDEX,
        DROP_KEYSPACE, DROP_COLUMNFAMILY, ALTER_TABLE;

    /** Statement types that don't require a keyspace to be set */
    private static final EnumSet<StatementType> TOP_LEVEL = EnumSet.of(USE, CREATE_KEYSPACE, DROP_KEYSPACE);

    /** Statement types that require a keyspace to be set */
    public static final EnumSet<StatementType> REQUIRES_KEYSPACE = EnumSet.complementOf(TOP_LEVEL);
}
