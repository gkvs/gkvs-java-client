package rocks.gkvs;

import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import rx.Observable;
import rx.Observable.OnSubscribe;


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
	
	
	public Observable<R> observe(final Observable<S> incoming) {
		
		return Observable.unsafeCreate(new OnSubscribe<R>() {

			@Override
			public void call(final rx.Subscriber<? super R> subscriber) {

				final Observer<S> sender = async(new Observer<R>() {

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
				
				incoming.subscribe(new rx.Observer<S>() {

					@Override
					public void onNext(S item) {
						sender.onNext(item);
					}

					@Override
					public void onError(Throwable t) {
						sender.onError(t);
					}

					@Override
					public void onCompleted() {
						sender.onCompleted();
					}
					
				});
				
			}
			
		});
		
	}
	
	
	/**
	 * Creates out-coming flux based on incoming one
	 * 
	 * @param incoming - flux
	 * @return output stream
	 */
	
	public Flux<R> flux(final Flux<S> incoming) {
		
		return Flux.create(new Consumer<FluxSink<R>>() {

			@Override
			public void accept(final FluxSink<R> sink) {

				final Observer<S> sender = async(new Observer<R>() {

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

				incoming.subscribe(new Subscriber<S>() {

					@Override
					public void onSubscribe(Subscription s) {
					}

					@Override
					public void onNext(S item) {
						sender.onNext(item);
					}

					@Override
					public void onError(Throwable t) {
						sender.onError(t);
					}

					@Override
					public void onComplete() {
						sender.onCompleted();
					}
					
				});

			}
			
		});
	}
	
}
