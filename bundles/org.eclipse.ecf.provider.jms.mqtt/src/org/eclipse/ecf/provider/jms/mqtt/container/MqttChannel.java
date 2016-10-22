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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.jms.JMSException;

import org.eclipse.ecf.core.identity.ID;
import org.eclipse.ecf.core.util.ECFException;
import org.eclipse.ecf.core.util.Trace;
import org.eclipse.ecf.provider.jms.identity.JMSID;
import org.eclipse.ecf.remoteservice.util.ObjectSerializationUtil;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MqttChannel implements MqttCallback {

	private MqttChannelMessageHandler handler;
	private MqttAsyncClient client;
	private String topic;
	private ExecutorService executorService;

	public MqttChannel(JMSID targetID, ID localID, MqttConnectOptions options, int qos, MqttChannelMessageHandler handler)
			throws ECFException {
		try {
			this.handler = handler;
			this.client = new MqttAsyncClient(targetID.getBroker(), localID.getName(), null);
			// Set callback
			this.client.setCallback(this);
			if (options == null)
				options = new MqttConnectOptions();
			// Connect to broker with connectOpts
			this.client.connect(options).waitForCompletion(options.getConnectionTimeout()*1000);
			this.topic = targetID.getTopicOrQueueName();
			// Subscribe to topic
			this.client.subscribe(topic, qos);
		} catch (MqttException e) {
			throw new ECFException("MqttClient could not connect to broker at targetID=" + targetID.getName(), e);
		}
		this.executorService = Executors.newSingleThreadExecutor(new ThreadFactory() {
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r,"Mqtt RemoteService Provider");
				t.setDaemon(true);
				return t;
			}});
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
			this.client.setCallback(null);
			try {
				this.client.disconnect();
				this.client.close();
			} catch (MqttException e) {
				e.printStackTrace();
			}
		}
		if (this.executorService != null) {
			this.executorService.shutdown();
			this.executorService = null;
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
		final MQTTMessage m = MQTTMessage.receive(message.getPayload());
		if (m == null) {
			Trace.exiting("org.eclipse.ecf.provider.jms.mqtt", "exiting", this.getClass(), "handleMessageArrived");
			// XXX here is where the MqttMessage payload could be passed to some
			// other interface
			return;
		} else {
			executorService.submit(new Runnable() {
				public void run() {
					handler.handleMqttChannelMessage(m.getData(), m.getCorrelationId());
				}
			});
		}
	}

}
