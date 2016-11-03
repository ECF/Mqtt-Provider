/*******************************************************************************
 * Copyright (c) 2015 Composent, Inc. and others. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Composent, Inc. - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.provider.jms.mqtt.container;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.ecf.core.IContainer;
import org.eclipse.ecf.core.util.ECFException;
import org.eclipse.ecf.provider.comm.ISynchAsynchConnection;
import org.eclipse.ecf.provider.jms.container.AbstractJMSServer;
import org.eclipse.ecf.provider.jms.container.JMSContainerConfig;
import org.eclipse.ecf.provider.jms.identity.JMSID;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

public class MqttJMSServerContainer extends AbstractJMSServer {

	public static final String DEFAULT_SERVER_ID = "tcp://iot.eclipse.org:1883/exampleTopic";
	public static final String MQTT_MANAGER_NAME = "ecf.jms.mqtt.manager";

	public static class Instantiator extends AbstractMqttContainerInstantiator {

		private static List<String> exporters = Arrays.asList(new String[] {MqttJMSServerContainer.MQTT_MANAGER_NAME, MqttJMSClientContainer.MQTT_CLIENT_NAME});
		private static Map<String,List<String>> exporterToImportersMap = new HashMap<String,List<String>>();
		
		static {
			exporterToImportersMap.put(MqttJMSServerContainer.MQTT_MANAGER_NAME, Arrays.asList(new String[] { MqttJMSClientContainer.MQTT_CLIENT_NAME }));
			exporterToImportersMap.put(MqttJMSClientContainer.MQTT_CLIENT_NAME, Arrays.asList(new String[] { MqttJMSServerContainer.MQTT_MANAGER_NAME }));		
		}
		
		public Instantiator() {
			super(exporters,exporterToImportersMap);
		}

		@Override
		protected IContainer createMqttContainer(JMSContainerConfig config, MqttConnectOptions options, int qos,
				Map<String, ?> parameters) throws Exception {
			MqttJMSServerContainer server = new MqttJMSServerContainer(config, options, qos);
			server.start();
			return server;
		}

		@Override
		protected JMSID createContainerID(Map<String, ?> parameters) throws Exception {
			return getJMSIDFromParameter(parameters.get(ID_PARAM), DEFAULT_SERVER_ID);
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

	@Override
	public void dispose() {
		disconnect();
		super.dispose();
	}
	private MqttConnectOptions mqttConnectOptions;
	private int qos;

	public MqttJMSServerContainer(JMSContainerConfig config, MqttConnectOptions connectOptions, int qos) {
		super(config);
		this.mqttConnectOptions = connectOptions;
		this.qos = qos;
	}

	@Override
	public void start() throws ECFException {
		final ISynchAsynchConnection connection = new MqttJMSServerChannel(this.getReceiver(),
				getJMSContainerConfig().getKeepAlive(), mqttConnectOptions, this.qos);
		setConnection(connection);
		connection.start();
	}

}
