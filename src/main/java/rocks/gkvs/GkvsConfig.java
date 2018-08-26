package rocks.gkvs;

import java.util.Properties;

/**
 * 
 * GkvsConfig
 *
 * @author Alex Shvid
 * @date Aug 26, 2018
 *
 */

public final class GkvsConfig {

	private final String host;
	private final int port;
	private final boolean useSsl;
	private final String sslKeys;

	protected GkvsConfig(Builder builder) {
		this.host = builder.host;
		this.port = builder.port;
		this.useSsl = builder.useSsl;
		this.sslKeys = builder.sslKeys;
	}
	
	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public boolean useSsl() {
		return useSsl;
	}

	public String getSslKeys() {
		return sslKeys;
	}



	public static final class Builder {

		private String host;
		private int port;
		private boolean useSsl;
		private String sslKeys;

		public String getHost() {
			return host;
		}

		public void setHost(String host) {
			this.host = host;
		}

		public int getPort() {
			return port;
		}

		public void setPort(int port) {
			this.port = port;
		}

		public boolean useSsl() {
			return useSsl;
		}

		public void useSsl(boolean useSsl) {
			this.useSsl = useSsl;
		}

		public String getSslKeys() {
			return sslKeys;
		}

		public void setSslKeys(String sslKeys) {
			this.sslKeys = sslKeys;
		}

		public GkvsConfig build() {
			return new GkvsConfig(this);
		}

	}

	public static GkvsConfig fromProperties(Properties props) {
		
		Builder builder = new Builder();
		
		builder.setHost(props.getProperty("gkvs.host", "localhost"));
		
		int port;
		try {
			port = Integer.parseInt(props.getProperty("gkvs.port", "4040"));
		}
		catch(NumberFormatException e) {
			throw new IllegalStateException("unable parse gkvs.port property", e);
		}
		
		builder.setPort(port);
		builder.useSsl(Boolean.parseBoolean(props.getProperty("gkvs.ssl.enabled", "false")));
	
		String keys = props.getProperty("gkvs.ssl.keys");
		if (keys == null) {
			keys = System.getProperty("GKVS_KEYS");			
		}
		if (keys == null) {
			keys = System.getenv("GKVS_KEYS");
		}
		if (keys == null) {
			keys = GkvsConstants.CLASSPATH_PREFIX;
		}
		
		builder.setSslKeys(keys);
		
		return builder.build();
	}

}
