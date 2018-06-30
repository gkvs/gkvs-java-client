/*
 *
 * Copyright 2018-present GKVS authors.
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
 *
 */
package rocks.gkvs.value;

import java.io.IOException;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.buffer.ArrayBufferOutput;
import org.msgpack.value.Value;

import rocks.gkvs.GkvsException;

/**
 * 
 * AbstractValueTest
 *
 * @author Alex Shvid
 * @date Jun 30, 2018 
 *
 */

public class AbstractValueTest {

	public byte[] toByteArray(Value value) {
		
		ArrayBufferOutput out = new ArrayBufferOutput();
		try {
			MessagePacker packer = MessagePack.newDefaultPacker(out);
			value.writeTo(packer);
			packer.flush();
		} catch (IOException e) {
			throw new GkvsException("serialization to byte array error", e);
		}

		return out.toByteArray();
	}
	
	public String toHexString(Value value) {
		return rocks.gkvs.value.Value.toHex(toByteArray(value));
	}

	public String toHexString(rocks.gkvs.value.Value value) {
		return value.toHexMsgpack();
	}
	
}
