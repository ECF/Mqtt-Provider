/*******************************************************************************
 * Copyright (c) 2015 Composent, Inc. and others. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Composent, Inc. - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.provider.jms.mqtt.container;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import javax.jms.JMSException;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MQTTMessage {

	private byte[] data;
	private String correlationId;

	private MQTTMessage(String correlationId, byte[] data) {
		this.data = data;
		this.correlationId = correlationId;
	}

	String getCorrelationId() {
		return this.correlationId;
	}

	byte[] getData() {
		return data;
	}

	static void send(final MqttClient client, final String topic, byte[] message, String jmsCorrelationId)
			throws JMSException {
		try {
			ByteArrayOutputStream bouts = new ByteArrayOutputStream();
			bouts.write(ECFPREFIX);
			ObjectOutputStream oos = new ObjectOutputStream(bouts);
			oos.writeObject(jmsCorrelationId);
			oos.writeObject(message);
			final MqttMessage m = new MqttMessage(bouts.toByteArray());
			m.setRetained(false);
			new Thread(new Runnable() {
				public void run() {
					try {
						client.publish(topic, m);
					} catch (MqttException e) {
						e.printStackTrace();
					}
				}
			}).start();
		} catch (Exception e) {
			JMSException t = new JMSException(e.getMessage());
			t.setStackTrace(e.getStackTrace());
			throw t;
		}

	}

	private static byte[] ECFPREFIX = { 27, 69, 67, 70 };

	static MQTTMessage receive(byte[] bytes) throws Exception {
		// Check the first four bytes
		if (!checkMessagePrefix(bytes))
			return null;
		// else it's an ECF message
		ByteArrayInputStream bins = new ByteArrayInputStream(bytes, ECFPREFIX.length, bytes.length - ECFPREFIX.length);
		ObjectInputStream oos = new ObjectInputStream(bins);
		return new MQTTMessage((String) oos.readObject(), (byte[]) oos.readObject());
	}

	private static boolean checkMessagePrefix(byte[] bytes) {
		for (int i = 0; i < ECFPREFIX.length; i++)
			if (ECFPREFIX[i] != bytes[i])
				return false;
		return true;
	}
}
