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
package org.apache.cassandra.dht;

import java.math.BigInteger;

public class BigIntegerToken extends Token<BigInteger>
{
    static final long serialVersionUID = -5833589141319293006L;

    public BigIntegerToken(BigInteger token)
    {
        super(token);
    }

    // convenience method for testing
    public BigIntegerToken(String token) {
        this(new BigInteger(token));
    }

    public int compareTo(Token<BigInteger> o)
    {
        return token.compareTo(o.token);
    }
}
