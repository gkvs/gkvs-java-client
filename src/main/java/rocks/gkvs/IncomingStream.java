package rocks.gkvs;

import java.util.Iterator;

/**
 * 
 * IncomingStream
 *
 * @author Alex Shvid
 * @date Aug 27, 2018 
 *
 * @param <R> - receive type
 */

public abstract class IncomingStream<R> {

	/**
	 * Sync streaming call to receive data
	 * 
	 * receives all data at once, not efficient for this interface
	 * 
	 * @return iterator of collection
	 */
	
	public abstract Iterator<R> sync();
	
	/**
	 * Async streaming call to receive data
	 * 
	 * @param observer
	 */
	
	public abstract void async(Observer<R> observer);
	
}
