package rocks.gkvs.value;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.msgpack.core.MessagePacker;
import org.msgpack.value.impl.ImmutableLongValueImpl;
import org.msgpack.value.impl.ImmutableMapValueImpl;
import org.msgpack.value.impl.ImmutableStringValueImpl;

import rocks.gkvs.GkvsException;

/**
 * 
 * Table
 *
 * @author Alex Shvid
 * @date Jun 27, 2018 
 *
 */

public final class Table extends Value {

	private final Map<String, Value> table = new HashMap<String, Value>();

	private TableType type = TableType.INT_KEY;

	public final class KeyComparator implements Comparator<String> {

		@Override
		public int compare(String o1, String o2) {

			if (type == TableType.INT_KEY) {

				try {
					int i1 = Integer.parseInt(o1);
					int i2 = Integer.parseInt(o2);

					return Integer.compare(i1, i2);

				} catch (NumberFormatException e) {
					return o1.compareTo(o2);
				}
			}

			return o1.compareTo(o2);
		}

	}

	public TableType getType() {
		return type;
	}

	public Value get(String key) {

		if (key == null) {
			throw new IllegalArgumentException("empty key");
		}

		return table.get(key);
	}

	public Table getTable(String key) {
		return Value.toTable(get(key));
	}

	public Bool getBool(String key) {
		return Value.toBool(get(key));
	}

	public Num getNum(String key) {
		return Value.toNum(get(key));
	}

	public Str getStr(String key) {
		return Value.toStr(get(key));
	}

	public Value get(int key) {
		return table.get(Integer.toString(key));
	}

	public Table getTable(int key) {
		return Value.toTable(get(key));
	}

	public Bool getBool(int key) {
		return Value.toBool(get(key));
	}

	public Num getNum(int key) {
		return Value.toNum(get(key));
	}

	public Str getStr(int key) {
		return Value.toStr(get(key));
	}

	public Value get(Field field) {

		if (field == null) {
			throw new IllegalArgumentException("null field");
		}

		if (field.isEmpty()) {
			return this;
		}

		int lastIndex = field.size() - 1;
		Table currentTable = navigateTable(field, false);

		if (currentTable != null) {
			String key = field.get(lastIndex);
			return currentTable.get(key);
		}
		
		return null;

	}

	public Table getTable(Field field) {
		return Value.toTable(get(field));
	}

	public Bool getBool(Field field) {
		return Value.toBool(get(field));
	}

	public Num getNum(Field field) {
		return Value.toNum(get(field));
	}

	public Str getStr(Field field) {
		return Value.toStr(get(field));
	}

	public Value put(String key, Value value) {

		if (value != null) {
			
			if (type == TableType.INT_KEY) {

				NumType numberType = Num.detectNumber(key);
				if (numberType != NumType.INT64) {
					type = TableType.STRING_KEY;
				}

			}
			
			return table.put(key, value);
		} else {
			return table.remove(key);
		}
	}

	public Value put(String key, String stringfyValue) {
		return put(key, Parser.parseStringifyValue(stringfyValue));
	}

	public Value put(int key, Value value) {
		if (value != null) {
			return table.put(Integer.toString(key), value);
		} else {
			return table.remove(Integer.toString(key));
		}
	}

	public Value put(int key, String stringfyValue) {
		return put(key, Parser.parseStringifyValue(stringfyValue));
	}

	public Value put(Field field, Value value) {

		if (field == null) {
			throw new IllegalArgumentException("null field");
		}

		if (field.isEmpty()) {
			return null;
		}

		Table currentTable = navigateTable(field, true);
		String key = field.get(field.size() - 1);
		if (value != null) {
			return currentTable.put(key, value);
		}
		else {
			return currentTable.remove(key);
		}
	}

	public Value put(Field field, String stringfyValue) {
		return put(field, Parser.parseStringifyValue(stringfyValue));
	}

	public Value remove(String key) {
		return table.remove(key);
	}

	public Value remove(int key) {
		return table.remove(Integer.toString(key));
	}

	public Value remove(Field field) {
		
		if (field == null) {
			throw new IllegalArgumentException("null field");
		}

		if (field.isEmpty()) {
			return null;
		}

		Table currentTable = navigateTable(field, true);
		String key = field.get(field.size() - 1);
		return currentTable.remove(key);
	}

	private Table navigateTable(Field field, boolean create) {
		
		int lastIndex = field.size() - 1;
		
		Table currentTable = this;
		for (int i = 0; i != lastIndex; ++i) {

			String key = field.get(i);
			Value existingValue = currentTable.get(key);

			if (existingValue == null || !(existingValue instanceof Table)) {
				if (create) {
					Table newTable = new Table();
					currentTable.put(key, newTable);
					currentTable = newTable;
				}
				else {
					return null;
				}
			} else {
				currentTable = (Table) existingValue;
			}

		}
		return currentTable;
	}
	
	public Set<String> keySet() {
		return table.keySet();
	}

	public List<Integer> intKeys() {

		List<Integer> list = new ArrayList<Integer>(table.size());

		for (String key : table.keySet()) {

			try {
				list.add(Integer.parseInt(key));
			} catch (NumberFormatException e) {
				// ignore
			}
		}

		Collections.sort(list);

		return list;
	}

	public Integer intMaxKey() {

		Integer maxKey = null;

		for (String key : table.keySet()) {

			try {
				int value = Integer.parseInt(key);
				if (maxKey == null || value > maxKey) {
					maxKey = value;
				}
			} catch (NumberFormatException e) {
				// ignore
			}
		}

		return maxKey;
	}

	public int size() {
		return table.size();
	}

	public void clear() {
		table.clear();
	}

	@Override
	public String asString() {
		StringBuilder str = new StringBuilder();
		str.append("{");
		boolean first = true;
		for (Map.Entry<String, Value> entry : table.entrySet()) {
			if (!first) {
				str.append(", ");
			}
			str.append(entry.getKey()).append("=").append(entry.getValue().asString());
			first = false;
		}
		str.append("}");
		return str.toString();
	}

	@Override
	public org.msgpack.value.Value toMsgpackValue() {

		switch (type) {

		case INT_KEY:
			return toIntValue();

		case STRING_KEY:
			return toStringValue();

		}

		throw new GkvsException("unexpected type: " + type);
	}

	private org.msgpack.value.Value toIntValue() {

		int size = size();

		int capacity = size << 1;
		org.msgpack.value.Value[] array = new org.msgpack.value.Value[capacity];

		int index = 0;
		for (Map.Entry<String, Value> entry : table.entrySet()) {

			int integerKey;
			try {
				integerKey = Integer.parseInt(entry.getKey());
			} catch (NumberFormatException e) {
				throw new GkvsException(entry.getKey(), e);
			}

			Value val = entry.getValue();

			array[index++] = new ImmutableLongValueImpl(integerKey);
			array[index++] = val.toMsgpackValue();

		}

		return new ImmutableMapValueImpl(array);

	}

	private org.msgpack.value.Value toStringValue() {

		int size = size();

		int capacity = size << 1;
		org.msgpack.value.Value[] array = new org.msgpack.value.Value[capacity];

		int index = 0;
		for (Map.Entry<String, Value> entry : table.entrySet()) {

			Value val = entry.getValue();

			array[index++] = new ImmutableStringValueImpl(entry.getKey());
			array[index++] = val.toMsgpackValue();

		}

		return new ImmutableMapValueImpl(array);

	}

	@Override
	public void writeTo(MessagePacker packer) throws IOException {
		switch (type) {

		case INT_KEY:
			writeIntMapTo(packer);
			break;

		case STRING_KEY:
			writeStringMapTo(packer);
			break;

		default:
			throw new IOException("unexpected type: " + type);
		}

	}

	private void writeIntMapTo(MessagePacker packer) throws IOException {

		int size = size();

		packer.packMapHeader(size);

		for (Map.Entry<String, Value> entry : table.entrySet()) {

			String key = entry.getKey();
			int intKey;
			try {
				intKey = Integer.parseInt(key);
			} catch (NumberFormatException e) {
				throw new IOException(key, e);
			}

			Value value = entry.getValue();

			packer.packInt(intKey);
			value.writeTo(packer);
		}

	}

	private void writeStringMapTo(MessagePacker packer) throws IOException {

		int size = size();

		packer.packMapHeader(size);

		for (Map.Entry<String, Value> entry : table.entrySet()) {

			String key = entry.getKey();
			Value value = entry.getValue();

			byte[] data = key.getBytes(StandardCharsets.UTF_8);
			packer.packRawStringHeader(data.length);
			packer.writePayload(data);

			value.writeTo(packer);
		}

	}

	@Override
	public void print(StringBuilder str, int initialSpaces, int tabSpaces) {
		str.append("Table [type=" + type + ", size=" + table.size() + "] {\n");
		boolean first = true;
		for (Map.Entry<String, Value> entry : table.entrySet()) {
			if (!first) {
				str.append(",\n");
			}
			addSpaces(str, initialSpaces + tabSpaces);
			str.append(entry.getKey()).append("=");
			entry.getValue().print(str, initialSpaces + tabSpaces, tabSpaces);
			first = false;
		}
		str.append("\n");
		addSpaces(str, initialSpaces);
		str.append("}");
	}

	private void addSpaces(StringBuilder str, int spaces) {
		for (int i = 0; i != spaces; ++i) {
			str.append(' ');
		}
	}

}
