/*******************************************************************************
 * Copyright (c) 2015 Composent, Inc. and others. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Composent, Inc. - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.provider.jms.mqtt.container;

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

		public Instantiator() {
			super(MQTT_MANAGER_NAME, MqttJMSClientContainer.MQTT_CLIENT_NAME);
		}

		@Override
		protected IContainer createMqttContainer(JMSContainerConfig config, MqttConnectOptions options,
				Map<String, ?> parameters) throws Exception {
			MqttJMSServerContainer server = new MqttJMSServerContainer(config, options);
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

	private MqttConnectOptions mqttConnectOptions;

	public MqttJMSServerContainer(JMSContainerConfig config, MqttConnectOptions connectOptions) {
		super(config);
		this.mqttConnectOptions = connectOptions;
	}

	@Override
	public void start() throws ECFException {
		final ISynchAsynchConnection connection = new MqttJMSServerChannel(this.getReceiver(),
				getJMSContainerConfig().getKeepAlive(), mqttConnectOptions);
		setConnection(connection);
		connection.start();
	}

}
