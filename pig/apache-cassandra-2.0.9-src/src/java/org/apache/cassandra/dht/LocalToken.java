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

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;

public class LocalToken extends Token<ByteBuffer>
{
    static final long serialVersionUID = 8437543776403014875L;

    private final AbstractType<?> comparator;

    public LocalToken(AbstractType<?> comparator, ByteBuffer token)
    {
        super(token);
        this.comparator = comparator;
    }

    @Override
    public String toString()
    {
        return comparator.getString(token);
    }

    public int compareTo(Token<ByteBuffer> o)
    {
        return comparator.compare(token, o.token);
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        return prime + token.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (!(obj instanceof LocalToken))
            return false;
        LocalToken other = (LocalToken) obj;
        return token.equals(other.token);
    }

}
