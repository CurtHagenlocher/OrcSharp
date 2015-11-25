/**
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

namespace org.apache.hadoop.hive.ql.io.orc
{
    using org.apache.hadoop.hive.ql.io.orc.external;

    class SnappyCodec : CompressionCodec, DirectDecompressionCodec
    {
        bool? direct;

        public bool compress(ByteBuffer @in, ByteBuffer @out, ByteBuffer overflow)
        {
            int inBytes = @in.remaining();
            // I should work on a patch for Snappy to support an overflow buffer
            // to prevent the extra buffer copy.
            byte[] compressed = new byte[Snappy.maxCompressedLength(inBytes)];
            int outBytes =
                Snappy.compress(@in.array(), @in.arrayOffset() + @in.position(), inBytes,
                    compressed, 0);
            if (outBytes < inBytes)
            {
                int remaining = @out.remaining();
                if (remaining >= outBytes)
                {
                    Array.Copy(compressed, 0, @out.array(), @out.arrayOffset() +
                        @out.position(), outBytes);
                    @out.position(@out.position() + outBytes);
                }
                else
                {
                    Array.Copy(compressed, 0, @out.array(), @out.arrayOffset() +
                        @out.position(), remaining);
                    @out.position(@out.limit());
                    Array.Copy(compressed, remaining, overflow.array(),
                        overflow.arrayOffset(), outBytes - remaining);
                    overflow.position(outBytes - remaining);
                }
                return true;
            }
            else
            {
                return false;
            }
        }

        public void decompress(ByteBuffer @in, ByteBuffer @out)
        {
            if (@in.isDirect() && @out.isDirect())
            {
                directDecompress(@in, @out);
                return;
            }
            int inOffset = @in.position();
            int uncompressLen =
                Snappy.uncompress(@in.array(), @in.arrayOffset() + inOffset,
                @in.limit() - inOffset, @out.array(), @out.arrayOffset() + @out.position());
            @out.position(uncompressLen + @out.position());
            @out.flip();
        }

        public bool isAvailable()
        {
            if (direct == null)
            {
                try
                {
                    if (ShimLoader.getHadoopShims().getDirectDecompressor(
                        DirectCompressionType.SNAPPY) != null)
                    {
                        direct = true;
                    }
                    else
                    {
                        direct = false;
                    }
                }
                catch (UnsatisfiedLinkError ule)
                {
                    direct = false;
                }
            }
            return direct.Value;
        }

        public void directDecompress(ByteBuffer @in, ByteBuffer @out)
        {
            DirectDecompressorShim decompressShim = ShimLoader.getHadoopShims()
                .getDirectDecompressor(DirectCompressionType.SNAPPY);
            decompressShim.decompress(@in, @out);
            @out.flip(); // flip for read
        }

        public CompressionCodec modify(EnumSet<Modifier> modifiers)
        {
            // snappy allows no modifications
            return this;
        }
    }
}
