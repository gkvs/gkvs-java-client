package rocks.gkvs;

/**
 * 
 * BiStream
 *
 * @author Alex Shvid
 * @date Aug 27, 2018 
 *
 * @param <S> - send type
 * @param <R> - receive type
 */

public abstract class BiStream<S, R> {

	/**
	 * Sync stream invocation works only when all data already places in to collections
	 * 
	 * @param src - input data
	 * @return output data
	 */
	
	public abstract Iterable<R> sync(Iterable<S> src);
	
	/**
	 * Bi-directional stream invocation
	 * 
	 * @param observer - observer of results
	 * @return observer of sending data
	 */
	
	public abstract Observer<S> async(Observer<R> observer);
	
}
