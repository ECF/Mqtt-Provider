package org.eclipse.ecf.provider.jms.mqtt.container;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.eclipse.ecf.core.ContainerCreateException;
import org.eclipse.ecf.core.ContainerTypeDescription;
import org.eclipse.ecf.core.IContainer;
import org.eclipse.ecf.core.identity.ID;
import org.eclipse.ecf.core.identity.IDFactory;
import org.eclipse.ecf.provider.comm.ConnectionCreateException;
import org.eclipse.ecf.provider.comm.ISynchAsynchConnection;
import org.eclipse.ecf.provider.generic.GenericContainerInstantiator;
import org.eclipse.ecf.provider.jms.container.AbstractJMSClient;
import org.eclipse.ecf.provider.jms.container.JMSContainerConfig;
import org.eclipse.ecf.provider.jms.identity.JMSID;
import org.eclipse.ecf.provider.jms.identity.JMSNamespace;
import org.eclipse.paho.client.mqttv3.MqttClient;

public class MqttJMSClientContainer extends AbstractJMSClient {

	public static final String MQTT_CLIENT_NAME = "ecf.jms.mqtt.client";

	public static class MqttJMSClientContainerInstantiator extends
			GenericContainerInstantiator {
		public static final String[] mqttIntents = { "MQTT" };
		public static final String ID_PARAM = "id";

		private JMSID getJMSIDFromParameter(Object p) {
			if (p instanceof String) {
				return (JMSID) IDFactory.getDefault().createID(
						JMSNamespace.NAME, (String) p);
			} else if (p instanceof JMSID) {
				return (JMSID) p;
			} else
				return null;
		}

		public IContainer createInstance(ContainerTypeDescription description,
				Object[] args) throws ContainerCreateException {
			try {
				JMSID clientID = null;
				if (args == null)
					clientID = getJMSIDFromParameter(MqttClient
							.generateClientId());
				else if (args.length > 0 && (args[0] instanceof Map)) {
					@SuppressWarnings("rawtypes")
					Map map = (Map) args[0];
					Object o = map.get(ID_PARAM);
					if (o != null && o instanceof String)
						clientID = getJMSIDFromParameter(o);
				} else if (args.length > 0)
					clientID = getJMSIDFromParameter(args[0]);
				if (clientID == null)
					clientID = getJMSIDFromParameter(MqttClient
							.generateClientId());
				Integer ka = null;
				if (args != null && args.length > 1)
					ka = getIntegerFromArg(args[1]);
				if (ka == null)
					ka = new Integer(MqttJMSServerContainer.DEFAULT_KEEPALIVE);
				return new MqttJMSClientContainer(new JMSContainerConfig(
						clientID, ka));
			} catch (Exception e) {
				throw new ContainerCreateException(
						"Exception creating activemq client container", e);
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
			if (MQTT_CLIENT_NAME.equals(description.getName())) {
				if (
				// If it's a normal manager
				supportedConfigs
						.contains(MqttJMSServerContainer.MQTT_MANAGER_NAME)
				// Or the service exporter is a client
						|| supportedConfigs.contains(MQTT_CLIENT_NAME)) {
					results.add(MQTT_CLIENT_NAME);
				}
			}
			if (results.size() == 0)
				return null;
			return (String[]) results.toArray(new String[] {});
		}

		public String[] getSupportedConfigs(ContainerTypeDescription description) {
			return new String[] { MQTT_CLIENT_NAME };
		}
	}

	public MqttJMSClientContainer(JMSContainerConfig config) {
		super(config);
	}

	@Override
	protected ISynchAsynchConnection createConnection(ID targetID, Object data)
			throws ConnectionCreateException {
		return new MqttJMSClientChannel(getReceiver(), getID().getName(), null);
	}

}
