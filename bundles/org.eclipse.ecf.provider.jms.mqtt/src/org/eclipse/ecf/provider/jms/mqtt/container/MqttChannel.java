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

import javax.jms.JMSException;

import org.eclipse.ecf.core.util.ECFException;
import org.eclipse.ecf.core.util.Trace;
import org.eclipse.ecf.provider.jms.identity.JMSID;
import org.eclipse.ecf.remoteservice.util.ObjectSerializationUtil;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MqttChannel implements MqttCallback {

	private MqttChannelMessageHandler handler;
	private MqttClient client;
	private String topic;

	public MqttChannel(JMSID targetID, MqttConnectOptions options, MqttChannelMessageHandler handler)
			throws ECFException {
		try {
			this.handler = handler;
			this.client = new MqttClient(targetID.getBroker(), MqttClient.generateClientId());
			// Set callback
			this.client.setCallback(this);
			// Connect to broker with connectOpts
			if (options == null)
				this.client.connect();
			else
				this.client.connect(options);
			this.topic = targetID.getTopicOrQueueName();
			// Subscribe to topic
			this.client.subscribe(topic);
		} catch (MqttException e) {
			throw new ECFException("MqttClient could not connect to broker at targetID=" + targetID.getName());
		}
	}

	public synchronized boolean isConnected() {
		return client.isConnected();
	}

	static final ObjectSerializationUtil osu = new ObjectSerializationUtil();

	public synchronized void sendMessage(Serializable object, String jmsCorrelationId) throws JMSException {
		byte[] serializedMessage;
		try {
			serializedMessage = osu.serializeToBytes(object);
		} catch (IOException e) {
			JMSException jmse = new JMSException(e.getMessage());
			jmse.setStackTrace(e.getStackTrace());
			throw jmse;
		}
		MQTTMessage.send(this.client, topic, serializedMessage, jmsCorrelationId);
	}

	public synchronized void disconnect() {
		if (this.client.isConnected()) {
			try {
				this.client.disconnect();
				this.client.close();
			} catch (MqttException e) {
				e.printStackTrace();
			}
		}
	}

	public void connectionLost(Throwable arg0) {
	}

	public void deliveryComplete(IMqttDeliveryToken arg0) {
	}

	public void messageArrived(String arg0, MqttMessage message) throws Exception {
		Trace.trace("org.eclipse.ecf.provider.jms.mqtt",
				"handleMessageArrived topic=" + topic + ", message=" + message);
		// First, verify that the topic is ours...otherwise, ignore
		String localTopic = this.topic;
		if (localTopic == null || !localTopic.equals(topic)) {
			Trace.trace("org.eclipse.ecf.provider.jms.mqtt",
					"handleMessageArrived.  Our topic=" + localTopic + " message topic=" + topic);
			return;
		}
		// Just log all persistent messages and return
		if (message.isRetained()) {
			Trace.trace("org.eclipse.ecf.provider.jms.mqtt",
					"handleMessageArrived.  Message=" + message + " retained, so not processing");
			return;
		}
		if (message.isDuplicate()) {
			Trace.trace("org.eclipse.ecf.provider.jms.mqtt",
					"handleMessageArrived.  Message=" + message + " is duplicate, so not processing");
			return;
		}
		MQTTMessage m = MQTTMessage.receive(message.getPayload());
		if (m == null) {
			Trace.exiting("org.eclipse.ecf.provider.jms.mqtt", "exiting", this.getClass(), "handleMessageArrived");
			// XXX here is where the MqttMessage payload could be passed to some
			// other interface
			return;
		} else
			handler.handleMqttChannelMessage(m.getData(), m.getCorrelationId());
	}

}
