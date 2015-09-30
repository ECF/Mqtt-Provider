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

import org.eclipse.ecf.core.identity.ID;
import org.eclipse.ecf.core.util.ECFException;
import org.eclipse.ecf.provider.comm.ConnectionEvent;
import org.eclipse.ecf.provider.comm.ISynchAsynchEventHandler;
import org.eclipse.ecf.provider.jms.channel.AbstractJMSServerChannel;
import org.eclipse.ecf.provider.jms.identity.JMSID;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

public class MqttJMSServerChannel extends AbstractJMSServerChannel implements MqttChannelMessageHandler {

	private static final long serialVersionUID = -5177220940980707980L;

	private MqttChannel channel;

	public MqttJMSServerChannel(ISynchAsynchEventHandler handler, int keepAlive, MqttConnectOptions options)
			throws ECFException {
		super(handler, keepAlive);
		this.channel = new MqttChannel((JMSID) getLocalID(), options, this);
	}

	@Override
	protected void createAndSendMessage(Serializable object, String jmsCorrelationId) throws JMSException {
		if (channel != null)
			this.channel.sendMessage(object, jmsCorrelationId);
	}

	public Client createClient(ID remoteID) {
		Client newclient = new Client(remoteID, false);
		newclient.start();
		return newclient;
	}

	@Override
	public boolean isConnected() {
		return (this.channel != null && this.channel.isConnected());
	}

	@Override
	public void disconnect() {
		if (this.channel != null) {
			this.channel.disconnect();
			this.channel = null;
		}
		synchronized (this.waitResponse) {
			waitResponse.notifyAll();
		}
		fireListenersDisconnect(new ConnectionEvent(this, null));
		connectionListeners.clear();
	}

	protected Object readObject(byte[] bytes) throws IOException, ClassNotFoundException {
		return MqttChannel.osu.deserializeFromBytes(bytes);
	}

	public void handleMqttChannelMessage(byte[] data, String correlationId) {
		super.handleMessage(data, correlationId);
	}

	@Override
	protected ConnectionFactory createJMSConnectionFactory(JMSID targetID) throws IOException {
		return null;
	}

}
