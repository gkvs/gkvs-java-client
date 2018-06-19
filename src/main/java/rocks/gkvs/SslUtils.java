package rocks.gkvs;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.net.ssl.SSLContext;

import io.netty.handler.ssl.SslProvider;
import rocks.gkvs.GkvsConstants.SslProviderLib;

/**
 * 
 * SslUtils
 *
 * Utils to simplify SSL initialization
 *
 * @author Alex Shvid
 * @date Jun 19, 2018
 *
 */

public final class SslUtils {

	private SslUtils() {
	}

	public static List<String> preferredCiphers() {
		try {
			List<String> list = Arrays.asList(SSLContext.getDefault().getDefaultSSLParameters().getCipherSuites());
			list.remove("_GCM_");
			return Collections.unmodifiableList(list);
		} catch (NoSuchAlgorithmException ex) {
			throw new GkvsException("ssl context error", ex);
		}
	}
	  
	public static SslProvider getSslProvider(SslProviderLib ssl) {
		switch(ssl) {
		case JDK:
			useConscryptIfAvailable();
			return SslProvider.JDK;
		case OPENSSL:
			return SslProvider.OPENSSL;
		case OPENSSL_REFCNT:
			return SslProvider.OPENSSL_REFCNT;
		}
		throw new IllegalArgumentException("unknown SSL provider " + ssl);
	}
	
	private static boolean conscryptInitialized;

	public static void useConscryptIfAvailable() {
		if (conscryptInitialized) {
			return;
		}
		
		synchronized (SslUtils.class) {
			if (conscryptInitialized) {
				return;
			}			
			initializeConscryptIfAvailable();
			conscryptInitialized = true;
		}
	}

	private static void initializeConscryptIfAvailable() {
		Class<?> conscrypt;
		try {
			conscrypt = Class.forName("org.conscrypt.Conscrypt");
		} catch (ClassNotFoundException ex) {
			return;
		}
		Method newProvider;
		try {
			newProvider = conscrypt.getMethod("newProvider");
		} catch (NoSuchMethodException ex) {
			throw new RuntimeException("Could not find newProvider method on Conscrypt", ex);
		}
		Provider provider;
		try {
			provider = (Provider) newProvider.invoke(null);
		} catch (IllegalAccessException ex) {
			throw new RuntimeException("Could not invoke Conscrypt.newProvider", ex);
		} catch (InvocationTargetException ex) {
			throw new RuntimeException("Could not invoke Conscrypt.newProvider", ex);
		}
		Security.insertProviderAt(provider, 1);
	}
}
