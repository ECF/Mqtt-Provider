/*******************************************************************************
 * Copyright (c) 2015 Composent, Inc. and others. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Composent, Inc. - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.provider.jms.mqtt.container;

import java.io.IOException;
import java.io.Serializable;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

import org.eclipse.ecf.core.util.ECFException;
import org.eclipse.ecf.provider.comm.ConnectionEvent;
import org.eclipse.ecf.provider.comm.ISynchAsynchEventHandler;
import org.eclipse.ecf.provider.jms.channel.AbstractJMSClientChannel;
import org.eclipse.ecf.provider.jms.identity.JMSID;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

public class MqttJMSClientChannel extends AbstractJMSClientChannel implements MqttChannelMessageHandler {

	private static final long serialVersionUID = -4250141332659030158L;

	private int qos;
	private MqttConnectOptions mqttConnectOptions;
	private MqttChannel channel;

	public MqttJMSClientChannel(ISynchAsynchEventHandler handler, int keepAlive, int qos, MqttConnectOptions options) {
		super(handler, keepAlive);
		this.qos = qos;
		this.mqttConnectOptions = options;
	}

	@Override
	public boolean isConnected() {
		return (this.channel != null && this.channel.isConnected());
	}

	@Override
	protected Serializable setupJMS(JMSID targetID, Object data) throws ECFException {
		this.channel = new MqttChannel(targetID, getLocalID(), this.mqttConnectOptions, this.qos, this);
		return (Serializable) data;
	}

	@Override
	protected void createAndSendMessage(Serializable object, String jmsCorrelationId) throws JMSException {
		if (this.channel != null)
			this.channel.sendMessage(object, jmsCorrelationId);
	}

	protected Object readObject(byte[] bytes) throws IOException, ClassNotFoundException {
		return MqttChannel.osu.deserializeFromBytes(bytes);
	}

	public void disconnect() {
		if (this.channel != null) {
			this.channel.disconnect();
			this.channel = null;
			this.mqttConnectOptions = null;
		}
		synchronized (this.waitResponse) {
			waitResponse.notifyAll();
		}
		fireListenersDisconnect(new ConnectionEvent(this, null));
		connectionListeners.clear();
	}

	@Override
	protected ConnectionFactory createJMSConnectionFactory(JMSID targetID) throws IOException {
		return null;
	}

	public void handleMqttChannelMessage(byte[] data, String correlation) {
		super.handleMessage(data, correlation);
	}

}
