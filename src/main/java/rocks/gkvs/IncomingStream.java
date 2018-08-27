package rocks.gkvs;

import java.util.Iterator;
import java.util.function.Consumer;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;

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
	
	/**
	 * Creates observable stream (RX-JAVA)
	 * 
	 * @return stream
	 */
	
	public Observable<R> observe() {
		
		return Observable.unsafeCreate(new OnSubscribe<R>() {

			@Override
			public void call(final Subscriber<? super R> subscriber) {

				async(new Observer<R>() {

					@Override
					public void onNext(R item) {
						subscriber.onNext(item);
					}

					@Override
					public void onError(Throwable t) {
						subscriber.onError(t);
					}

					@Override
					public void onCompleted() {
						subscriber.onCompleted();
					}
					
				});
				
			}
			
		});
		
	}
	
	
	/**
	 * Creates Flux stream for the result (REACTOR)
	 * 
	 * @return flux
	 */
	
	public Flux<R> flux() {
		
		return Flux.create(new Consumer<FluxSink<R>>() {

			@Override
			public void accept(final FluxSink<R> sink) {

				async(new Observer<R>() {

					@Override
					public void onNext(R item) {
						sink.next(item);
					}

					@Override
					public void onError(Throwable t) {
						sink.error(t);
					}

					@Override
					public void onCompleted() {
						sink.complete();
					}
					
				});
				
			}
			
		});
		
	}
	
}
