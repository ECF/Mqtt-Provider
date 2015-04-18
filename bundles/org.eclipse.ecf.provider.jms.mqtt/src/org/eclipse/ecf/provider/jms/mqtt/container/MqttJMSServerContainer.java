package org.eclipse.ecf.provider.jms.mqtt.container;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.eclipse.ecf.core.ContainerCreateException;
import org.eclipse.ecf.core.ContainerTypeDescription;
import org.eclipse.ecf.core.IContainer;
import org.eclipse.ecf.core.identity.IDFactory;
import org.eclipse.ecf.core.util.ECFException;
import org.eclipse.ecf.provider.comm.ISynchAsynchConnection;
import org.eclipse.ecf.provider.generic.GenericContainerInstantiator;
import org.eclipse.ecf.provider.jms.container.AbstractJMSServer;
import org.eclipse.ecf.provider.jms.container.JMSContainerConfig;
import org.eclipse.ecf.provider.jms.identity.JMSID;
import org.eclipse.ecf.provider.jms.identity.JMSNamespace;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

public class MqttJMSServerContainer extends AbstractJMSServer {

	public static final String DEFAULT_SERVER_ID = "tcp://m2m.eclipse.org:1883/exampleTopic";

	public static final String ID_PARAM = "id";
	public static final String MQTT_CLIENTID_PARAM = "mqttClientId";

	protected static final String[] mqttIntents = { "MQTT" };

	protected static final String MQTT_MANAGER_NAME = "ecf.jms.mqtt.manager";

	public static class MqttJMSServerContainerInstantiator extends
			GenericContainerInstantiator {
		private JMSID getJMSIDFromParameter(Object p) {
			if (p instanceof String) {
				return (JMSID) IDFactory.getDefault().createID(
						JMSNamespace.NAME, (String) p);
			} else if (p instanceof JMSID) {
				return (JMSID) p;
			} else
				return (JMSID) IDFactory.getDefault().createID(
						JMSNamespace.NAME, DEFAULT_SERVER_ID);
		}

		@SuppressWarnings("rawtypes")
		public IContainer createInstance(ContainerTypeDescription description,
				Object[] args) throws ContainerCreateException {
			try {
				JMSID serverID = null;
				String mqttClientId = null;
				Map props = null;
				Integer ka = null;
				if (args == null)
					serverID = getJMSIDFromParameter((String) DEFAULT_SERVER_ID);
				else if (args.length > 0) {
					if (args[0] instanceof Map) {
						props = (Map) args[0];
						Object o = props.get(ID_PARAM);
						if (o != null && o instanceof String)
							serverID = getJMSIDFromParameter(o);
						o = props.get(MQTT_CLIENTID_PARAM);
						if (o != null && o instanceof String)
							mqttClientId = (String) o;
						
					} else {
						serverID = getJMSIDFromParameter(args[0]);
						if (args.length > 1)
							ka = getIntegerFromArg(args[1]);
					}
				}
				if (ka == null)
					ka = new Integer(DEFAULT_KEEPALIVE);
				MqttJMSServerContainer server = new MqttJMSServerContainer(
						new JMSContainerConfig(serverID, ka.intValue(), props),
						mqttClientId, new MqttConnectOptions());
				server.start();
				return server;
			} catch (Exception e) {
				throw new ContainerCreateException(
						"Exception creating activemq server container", e);
			}
		}

		public String[] getSupportedIntents(ContainerTypeDescription description) {
			List<String> results = new ArrayList<String>();
			for (int i = 0; i < genericProviderIntents.length; i++) {
				results.add(genericProviderIntents[i]);
			}
			for (int i = 0; i < mqttIntents.length; i++) {
				results.add(mqttIntents[i]);
			}
			return (String[]) results.toArray(new String[] {});
		}

		public String[] getImportedConfigs(
				ContainerTypeDescription description,
				String[] exporterSupportedConfigs) {
			List<String> results = new ArrayList<String>();
			List<String> supportedConfigs = Arrays
					.asList(exporterSupportedConfigs);
			// For a manager, if a client is exporter then we are an importer
			if (MQTT_MANAGER_NAME.equals(description.getName())) {
				if (supportedConfigs
						.contains(MqttJMSClientContainer.MQTT_CLIENT_NAME))
					results.add(MQTT_MANAGER_NAME);
			}
			if (results.size() == 0)
				return null;
			return (String[]) results.toArray(new String[] {});
		}

		public String[] getSupportedConfigs(ContainerTypeDescription description) {
			return new String[] { MQTT_MANAGER_NAME };
		}

	}

	@Override
	public void disconnect() {
		super.disconnect();
		ISynchAsynchConnection conn = getConnection();
		if (conn != null)
			conn.disconnect();
		setConnection(null);
	}

	private String mqttClientId;
	private MqttConnectOptions mqttConnectOptions;

	public MqttJMSServerContainer(JMSContainerConfig config,
			String mqttClientId, MqttConnectOptions connectOptions) {
		super(config);
		this.mqttClientId = mqttClientId;
		this.mqttConnectOptions = connectOptions;
	}

	@Override
	public void start() throws ECFException {
		final ISynchAsynchConnection connection = new MqttJMSServerChannel(
				this.getReceiver(), mqttClientId, mqttConnectOptions);
		setConnection(connection);
		connection.start();
	}

}
