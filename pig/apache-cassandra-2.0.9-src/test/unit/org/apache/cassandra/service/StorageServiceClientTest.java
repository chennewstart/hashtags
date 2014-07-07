/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.cassandra.service;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.junit.Test;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.IOException;

public class StorageServiceClientTest
{
    @Test
    public void testClientOnlyMode() throws IOException, ConfigurationException
    {
        SchemaLoader.mkdirs();
        SchemaLoader.cleanup();
        StorageService.instance.initClient(0);

        // verify that no storage directories were created.
        for (String path : DatabaseDescriptor.getAllDataFileLocations())
        {
            assertFalse(new File(path).exists());
        }
        StorageService.instance.stopClient();
    }
}
