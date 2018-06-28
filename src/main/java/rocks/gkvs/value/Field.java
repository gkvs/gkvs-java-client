package rocks.gkvs.value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

/**
 * 
 * Field
 *
 * @author Alex Shvid
 * @date Jun 27, 2018 
 *
 */

public final class Field {

	private final List<String> path = new ArrayList<String>();

	public Field(String fieldPath) {
		
		if (fieldPath == null) {
			throw new IllegalArgumentException("empty field");
		}

		StringTokenizer tokenizer = new StringTokenizer(fieldPath, ".[]");
		
		while(tokenizer.hasMoreTokens()) {
			
			String token = tokenizer.nextToken().trim();
			
			if (!token.isEmpty()) {
				path.add(token);
			}
			
		}
		
	}

	public boolean isEmpty() {
		return path.isEmpty();
	}
	
	public int size() {
		return path.size();
	}
	
	public String get(int i) {
		return path.get(i);
	}
	
	public List<String> getPath() {
		return Collections.unmodifiableList(path);
	}

	public String asString() {
		StringBuilder str = new StringBuilder();
		for (String element : path) {
			NumType numberType = Num.detectNumber(element);
			if (numberType == NumType.INT64) {
				str.append("[").append(element).append("]");
			}
			else {
				if (str.length() > 0) {
					str.append(".");
				}
				str.append(element);
			}
		}
		return str.toString();
	}
	
	@Override
	public String toString() {
		return asString();
	}
	
}
