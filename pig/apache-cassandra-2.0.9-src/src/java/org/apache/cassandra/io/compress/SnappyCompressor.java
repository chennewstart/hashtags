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
package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyError;

public class SnappyCompressor implements ICompressor
{
    public static final SnappyCompressor instance = new SnappyCompressor();

    private static Logger logger = LoggerFactory.getLogger(SnappyCompressor.class);
    static
    {
        if (!isAvailable())
            logger.warn("Cannot initialize native Snappy library. Compression on new sstables will be disabled.");
    }

    public static SnappyCompressor create(Map<String, String> compressionOptions)
    {
        // this would throw java.lang.NoClassDefFoundError if Snappy class
        // wasn't found at runtime which should be processed by calling method
        Snappy.getNativeLibraryVersion();

        // no specific options supported so far
        return instance;
    }

    public static boolean isAvailable()
    {
        try
        {
            create(Collections.<String, String>emptyMap());
            return true;
        }
        catch (Exception e)
        {
            return false;
        }
        catch (NoClassDefFoundError e)
        {
            return false;
        }
        catch (SnappyError e)
        {
            return false;
        }
        catch (UnsatisfiedLinkError e)
        {
            return false;
        }
    }

    public Set<String> supportedOptions()
    {
        return Collections.emptySet();
    }

    public int initialCompressedBufferLength(int chunkLength)
    {
        return Snappy.maxCompressedLength(chunkLength);
    }

    public int compress(byte[] input, int inputOffset, int inputLength, ICompressor.WrappedArray output, int outputOffset) throws IOException
    {
        return Snappy.rawCompress(input, inputOffset, inputLength, output.buffer, outputOffset);
    }

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        return Snappy.rawUncompress(input, inputOffset, inputLength, output, outputOffset);
    }
}
