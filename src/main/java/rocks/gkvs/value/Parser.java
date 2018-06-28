package rocks.gkvs.value;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import rocks.gkvs.GkvsException;

/**
 * 
 * Parser
 *
 * @author Alex Shvid
 * @date Jun 27, 2018 
 *
 */

public final class Parser {

	private Parser() {
	}

	@SuppressWarnings("unchecked")
	public static <T extends Value> T parseTypedValue(byte[] buffer) {
		if (buffer != null) {
			return (T) parseValue(buffer, 0, buffer.length);
		}
		return null;
	}

	public static Value parseValue(byte[] buffer) {
		if (buffer != null) {
			return parseValue(buffer, 0, buffer.length);
		}
		return null;
	}

	public static Value parseValue(byte[] buffer, int offset, int length) {
		MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(buffer, offset, length);
		try {
			return parseValue(unpacker);
		} catch (IOException e) {
			throw new GkvsException("unexpected IOException", e);
		}
	}

	public static Value parseValue(ByteBuffer buffer) {
		MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(buffer);
		try {
			return parseValue(unpacker);
		} catch (IOException e) {
			throw new GkvsException("unexpected IOException", e);
		}
	}
	
	public static Value parseValue(InputStream in) {
		MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(in);
		try {
			return parseValue(unpacker);
		} catch (IOException e) {
			throw new GkvsException("unexpected IOException", e);
		}
	}

	public static Value parseValue(MessageUnpacker unpacker) throws IOException {

		if (!unpacker.hasNext()) {
			return null;
		}

		MessageFormat format = unpacker.getNextFormat();

		if (isNull(format)) {
			unpacker.unpackNil();
			return null;
		}

		else if (isArray(format)) {
			return parseArray(unpacker);
		}

		else if (isMap(format)) {
			return parseMap(unpacker);
		}

		else {
			return parseSimpleValue(format, unpacker);
		}

	}

	private static Value parseArray(MessageUnpacker unpacker) throws IOException {

		Table table = new Table();

		int arraySize = unpacker.unpackArrayHeader();
		if (arraySize == 0) {
			return table;
		}

		for (int i = 0; i != arraySize; ++i) {

			Value value = parseValue(unpacker);

			if (value != null) {
				table.put(i, value);
			}

		}

		return table;
	}

	private static Value parseMap(MessageUnpacker unpacker) throws IOException {

		Table table = new Table();

		int mapSize = unpacker.unpackMapHeader();
		if (mapSize == 0) {
			return table;
		}

		for (int i = 0; i != mapSize; ++i) {

			Value key = parseValue(unpacker);
			Value value = parseValue(unpacker);

			if (key != null && value != null) {

				if (key instanceof Num && table.getType() == TableType.INT_KEY) {
					Num number = (Num) key;
					table.put((int) number.asLong(), value);
				} else {
					table.put(key.asString(), value);
				}
			}

		}

		return table;
	}

	private static Value parseSimpleValue(MessageFormat format, MessageUnpacker unpacker) throws IOException {

		switch (format) {

		case BOOLEAN:
			return new Bool(unpacker.unpackBoolean());

		case INT8:
		case INT16:
		case INT32:
		case INT64:
		case UINT8:
		case UINT16:
		case UINT32:
		case UINT64:
		case POSFIXINT:
		case NEGFIXINT:
			return new Num(unpacker.unpackLong());

		case FLOAT32:
		case FLOAT64:
			return new Num(unpacker.unpackDouble());

		case STR8:
		case STR16:
		case STR32:
		case FIXSTR:
			return new Str(unpacker.unpackString());

		case BIN8:
		case BIN16:
		case BIN32:
			return new Str(unpacker.readPayload(unpacker.unpackBinaryHeader()), false);

		default:
			unpacker.skipValue();
			return null;
		}

	}

	public static Value parseStringifyValue(String stringifyValue) {

		if (stringifyValue == null) {
			return null;
		}

		if (stringifyValue.equalsIgnoreCase("true")) {
			return new Bool(true);
		}

		if (stringifyValue.equalsIgnoreCase("false")) {
			return new Bool(false);
		}

		NumType type = Num.detectNumber(stringifyValue);

		if (type == null) {
			return new Str(stringifyValue);
		} else {

			switch (type) {

			case INT64:
				try {
					return new Num(Long.parseLong(stringifyValue));
				} catch (NumberFormatException e) {
					throw new GkvsException(stringifyValue, e);
				}

			case FLOAT64:
				try {
					return new Num(Double.parseDouble(stringifyValue));
				} catch (NumberFormatException e) {
					throw new GkvsException(stringifyValue, e);
				}

			default:
				throw new GkvsException("invalid type: " + type + ", for stringfy value: " + stringifyValue);
			}

		}

	}

	public static boolean isArray(MessageFormat format) {

		switch (format) {

		case FIXARRAY:
		case ARRAY16:
		case ARRAY32:
			return true;

		default:
			return false;

		}

	}

	public static boolean isMap(MessageFormat format) {

		switch (format) {

		case FIXMAP:
		case MAP16:
		case MAP32:
			return true;

		default:
			return false;

		}

	}

	public static boolean isNull(MessageFormat format) {
		return MessageFormat.NIL == format;
	}

}
