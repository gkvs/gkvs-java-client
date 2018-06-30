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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;

/**
 * 
 * SparseList
 *
 * @author Alex Shvid
 * @date Jun 29, 2018 
 *
 * @param <E>
 */

public final class SparseList<E> implements Cloneable, Serializable {

	private static final long serialVersionUID = -5247859581162107053L;

	// C, C++, Java is 0, LUA is 1
	public static final int BASE = 0;
	
	private boolean hasGarbage = false;

	private final static Object TOMBSTONE = new Object();

	transient int[] sortedKeys;
	transient Object[] elementData;
	
	private int size;
	private final int base;

	public SparseList() {
		this(10, BASE);
	}

	public SparseList(int initialCapacity) {
		this(initialCapacity, BASE);
	}
	
	public SparseList(int initialCapacity, int base) {
		this.elementData = new Object[initialCapacity];
		this.sortedKeys = new int[elementData.length];
		this.size = 0;
		this.base = base;
	}

	@Override
	@SuppressWarnings("unchecked")
	public SparseList<E> clone() {
		if (hasGarbage) {
			compact();
		}
		SparseList<E> clone = null;
		try {
			clone = (SparseList<E>) super.clone();
			clone.sortedKeys = Arrays.copyOf(sortedKeys, size);
			clone.elementData = Arrays.copyOf(elementData, size);
		} catch (CloneNotSupportedException cnse) {
			/* ignore */
		}
		return clone;
	}
	
    @SuppressWarnings("unchecked")
    E elementData(int index) {
        return (E) elementData[index];
    }

	public E get(int key) {
		return get(key, null);
	}

	public E get(int key, E defaultValue) {
		int i = Arrays.binarySearch(sortedKeys, 0, size, key);
		if (i < 0 || elementData[i] == TOMBSTONE) {
			return defaultValue;
		} else {
			return elementData(i);
		}
	}
    
	public E remove(int key) {
		int i = Arrays.binarySearch(sortedKeys, 0, size, key);
		if (i >= 0) {
			if (elementData[i] != TOMBSTONE) {
				final E old = elementData(i);
				elementData[i] = TOMBSTONE;
				hasGarbage = true;
				return old;
			}
		}
		return null;
	}
		
	public boolean remove(Object value) {
		int i = indexOf(value);
		if (i >= 0) {
			return removeAt(i);
		}
		return false;
	}

	public boolean removeAt(int index) {
		checkRange(index);
		if (elementData[index] != TOMBSTONE) {
			elementData[index] = TOMBSTONE;
			hasGarbage = true;
			return true;
		}
		return false;
	}
	
	public boolean removeAll(Collection<?> c) {
		boolean updated = false;	
		for (Object value : c) {
			updated = updated || remove(value);
		}
		return updated;
	}

	public boolean isEmpty() {
		return size() == 0;
	}

	public int size() {
		if (hasGarbage) {
			compact();
		}
		return size;
	}

	public int keyAt(int index) {
		if (hasGarbage) {
			compact();
		}
		return sortedKeys[checkRange(index)];
	}

	public E valueAt(int index) {
		if (hasGarbage) {
			compact();
		}
		return elementData(checkRange(index));
	}

	public void setValueAt(int index, E value) {
		if (hasGarbage) {
			compact();
		}
		elementData[checkRange(index)] = value;
	}

	public int indexOfKey(int key) {
		if (hasGarbage) {
			compact();
		}
		return Arrays.binarySearch(sortedKeys, 0, size, key);
	}

	public int indexOfRef(Object value) {
		if (hasGarbage) {
			compact();
		}
		for (int i = 0; i < size; i++) {
			if (elementData[i] == value) {
				return i;
			}
		}
		return -1;
	}

	public int indexOf(Object value) {
		if (hasGarbage) {
			compact();
		}
		if (value == null) {
			for (int i = 0; i < size; i++) {
				if (elementData[i] == null) {
					return i;
				}
			}
		}
		else {
			for (int i = 0; i < size; i++) {
				if (value.equals(elementData[i])) {
					return i;
				}
			}
		}
		return -1;
	}
	
    public int lastIndexOf(Object value) {
		if (hasGarbage) {
			compact();
		}
        if (value == null) {
            for (int i = size-1; i >= 0; i--)
                if (elementData[i]==null) {
                    return i;
                }
        } else {
            for (int i = size-1; i >= 0; i--)
                if (value.equals(elementData[i])) {
                    return i;
                }
        }
        return -1;
    }


	public void clear() {
		int n = size;
		Object[] values = elementData;
		for (int i = 0; i < n; i++) {
			values[i] = null;
		}
		size = 0;
		hasGarbage = false;
	}

	public boolean add(E value) {
		add(lastKey()+1, value);
		return true;
	}

	public void add(int key, E value) {
		if (key < 0) {
			throw new IllegalArgumentException("only positive keys are allowed: " + key);
		}
		// append
		if (size == 0 || key > sortedKeys[size - 1]) {
			doAppend(key, value);
			return;
		}
		// insert
		int i = Arrays.binarySearch(sortedKeys, 0, size, key);
		if (i >= 0) {
			doInsertExisting(i, key, value);
		}
		else {
			doInsertAbsent(~i, key, value);
		}
	}

	public E set(int key, E value) {
		if (key < 0) {
			throw new IllegalArgumentException("only positive keys are allowed: " + key);
		}
		// append
		if (size == 0 || key > sortedKeys[size - 1]) {
			doAppend(key, value);
			return null;
		}
		// replace
		int i = Arrays.binarySearch(sortedKeys, 0, size, key);
		if (i >= 0) {
			final E old = elementData(i);
			elementData[i] = value;
			return old;
		} else {
			doInsertAbsent(~i, key, value);
			return null;
		}
	}
	
	private void doAppend(int key, E value) {
		if (hasGarbage && size >= sortedKeys.length) {
			compact();
		}
		sortedKeys = append(sortedKeys, size, key);
		elementData = append(elementData, size, value);
		size++;
	}
	
	private void doInsertAbsent(int i, int key, E value) {
		if (i < size && elementData[i] == TOMBSTONE) {
			// reuse deleted neighbor
			sortedKeys[i] = key;
			elementData[i] = value;
			return;
		}
		if (hasGarbage && size >= sortedKeys.length) {
			compact();
			i = ~Arrays.binarySearch(sortedKeys, 0, size, key);
		}
		sortedKeys = insert(sortedKeys, size, i, key);
		elementData = insert(elementData, size, i, value);
		size++;
		return;
	}
	
	private void doInsertExisting(int i, int key, E value) {
		int oldKey = sortedKeys[i];
		E oldValue = elementData(i);
		
		sortedKeys[i] = key;
		elementData[i] = value;
		
		if (oldValue != TOMBSTONE) {
			add(oldKey+1, oldValue);
		}
		return;
	}

	private int checkRange(int index) {
		if (index < 0 || index >= size) {
			throw new IndexOutOfBoundsException(size > 0 ? "index is out of range: [0," + size + ")" : "empty array");
		}
		return index;
	}
	
	public int firstKey() {
		return size == 0 ? 0 : sortedKeys[0];
	}

	public int lastKey() {
		return size == 0 ? base-1 : sortedKeys[size-1];
	}
	
	public boolean isSequence() {
		return isSequence(base);
	}
	
	public int base() {
		return base;
	}
	
	public boolean isSequence(int base) {
		int first = firstKey();
		if (first != base) {
			return false;
		}
		return lastKey() - first == size - 1;
	}
	
	private Object[] insert(Object[] array, int currentSize, int index, E element) {
		if (currentSize + 1 <= array.length) {
			if (currentSize - index > 0) {
				System.arraycopy(array, index, array, index + 1, currentSize - index);
			}
			array[index] = element;
			return array;
		}
		Object[] newArray = new Object[grow(currentSize)];
		System.arraycopy(array, 0, newArray, 0, index);
		newArray[index] = element;
		System.arraycopy(array, index, newArray, index + 1, array.length - index);
		return newArray;
	}

	private int[] insert(int[] array, int currentSize, int index, int element) {
		if (currentSize + 1 <= array.length) {
			if (currentSize - index > 0) {
				System.arraycopy(array, index, array, index + 1, currentSize - index);
			}
			array[index] = element;
			return array;
		}
		int[] newArray = new int[grow(currentSize)];
		System.arraycopy(array, 0, newArray, 0, index);
		newArray[index] = element;
		System.arraycopy(array, index, newArray, index + 1, array.length - index);
		return newArray;
	}

	private Object[] append(Object[] array, int currentSize, E element) {
		if (currentSize + 1 > array.length) {
			Object[] newArray = new Object[grow(currentSize)];
			System.arraycopy(array, 0, newArray, 0, currentSize);
			array = newArray;
		}
		array[currentSize] = element;
		return array;
	}

	private int[] append(int[] array, int currentSize, int element) {
		if (currentSize + 1 > array.length) {
			int[] newArray = new int[grow(currentSize)];
			System.arraycopy(array, 0, newArray, 0, currentSize);
			array = newArray;
		}
		array[currentSize] = element;
		return array;
	}

	public static int grow(int currentSize) {
		return currentSize <= 10 ? 24 : currentSize * 2;
	}

	private void compact() {
		int[] keys = sortedKeys;
		Object[] values = elementData;
		int j = 0;
		int n = size;
		for (int i = 0; i < n; i++) {
			Object val = values[i];
			if (val != TOMBSTONE) {
				if (i != j) {
					keys[j] = keys[i];
					values[j] = val;
					values[i] = null;
				}
				j++;
			}
		}
		hasGarbage = false;
		size = j;
	}
	
	@Override
	public String toString() {
		
		if (hasGarbage) {
			compact();
		}
		
		if (isEmpty()) {
			return "[]";
		}
		
		StringBuilder buffer = new StringBuilder(size * 20);
		
		if (isSequence()) {
			buffer.append('[');
			
			for (int i = 0; i < size; i++) {
				Object value = valueAt(i);
				if (i != 0) {
					buffer.append(", ");
				}
				if (value != this) {
					buffer.append(value);
				} else {
					buffer.append("this");
				}
			}
			
			buffer.append(']');
		}
		else {
			buffer.append('{');
			
			for (int i = 0; i < size; i++) {
				if (i != 0) {
					buffer.append(", ");
				}
				int key = keyAt(i);
				buffer.append(key);
				buffer.append('=');
				Object value = valueAt(i);
				if (value != this) {
					buffer.append(value);
				} else {
					buffer.append("this");
				}
			}
			
			buffer.append('}');
		}
		
		return buffer.toString();
	}

}
