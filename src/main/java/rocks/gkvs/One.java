package rocks.gkvs;

import java.util.function.Consumer;

import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import rx.Single;
import rx.SingleSubscriber;
import rx.Single.OnSubscribe;

/**
 * 
 * One
 *
 * Single result receiver
 *
 * @author Alex Shvid
 * @date Aug 27, 2018 
 *
 * @param <T>
 */

public abstract class One<T> {

	/**
	 * Gets result synchronously
	 * 
	 * @return result object
	 */
	
	public abstract T sync();
	
	/**
	 * Gets completable future of the result
	 * 
	 * @return completable future
	 */
	
	public abstract GkvsFuture<T> async();
	
	public abstract void async(Observer<T> observer);

	/**
	 * Gets RX-JAVA Single result
	 * 
	 * @return single object
	 */
	
	public Single<T> single() {

		return Single.create(new OnSubscribe<T>() {

			@Override
			public void call(final SingleSubscriber<? super T> subscriber) {
				
				async(new Observer<T>() {

					@Override
					public void onNext(T item) {
						subscriber.onSuccess(item);
					}

					@Override
					public void onError(Throwable t) {
						subscriber.onError(t);
					}

					@Override
					public void onCompleted() {
					}
					
				});
				
			}
			
		});
		
	}
	
	/**
	 * Gets Reactor Mono result
	 * 
	 * @return mono result
	 */
	
	public Mono<T> mono() {
	
		return Mono.create(new Consumer<MonoSink<T>>() {

			@Override
			public void accept(final MonoSink<T> sink) {
				
				async(new Observer<T>() {

					@Override
					public void onNext(T item) {
						sink.success(item);
					}

					@Override
					public void onError(Throwable t) {
						sink.error(t);
					}

					@Override
					public void onCompleted() {
						// will be ignored if second
						sink.success();
					}
					
				});
				
			}
			
			
		});
		
	}
	
}
