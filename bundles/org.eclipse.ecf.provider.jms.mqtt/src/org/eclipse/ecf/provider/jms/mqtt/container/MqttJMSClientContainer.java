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
import java.util.UUID;

import org.eclipse.ecf.core.IContainer;
import org.eclipse.ecf.core.identity.ID;
import org.eclipse.ecf.provider.comm.ConnectionCreateException;
import org.eclipse.ecf.provider.comm.ISynchAsynchConnection;
import org.eclipse.ecf.provider.jms.container.AbstractJMSClient;
import org.eclipse.ecf.provider.jms.container.JMSContainerConfig;
import org.eclipse.ecf.provider.jms.identity.JMSID;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

public class MqttJMSClientContainer extends AbstractJMSClient {

	public static final String MQTT_CLIENT_NAME = "ecf.jms.mqtt.client";

	public static class Instantiator extends AbstractMqttContainerInstantiator {

		public Instantiator() {
			super(MqttJMSServerContainer.MQTT_MANAGER_NAME, MqttJMSClientContainer.MQTT_CLIENT_NAME);
		}

		@Override
		protected IContainer createMqttContainer(JMSContainerConfig config, MqttConnectOptions options, int qos,
				Map<String, ?> parameters) throws Exception {
			return new MqttJMSClientContainer(config, options, qos);
		}

		@Override
		protected JMSID createContainerID(Map<String, ?> parameters) throws Exception {
			return getJMSIDFromParameter(parameters, UUID.randomUUID().toString());
		}
	}

	private int qos;
	private MqttConnectOptions options;

	MqttJMSClientContainer(JMSContainerConfig config, MqttConnectOptions options, int qos) {
		super(config);
		this.options = options;
		this.qos = qos;
	}

	@Override
	protected ISynchAsynchConnection createConnection(ID targetID, Object data) throws ConnectionCreateException {
		return new MqttJMSClientChannel(getReceiver(), getJMSContainerConfig().getKeepAlive(), this.qos, options);
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
		super.disconnect();
	}
	
	@Override
	public void dispose() {
		// TODO Auto-generated method stub
		super.dispose();
	}
}
