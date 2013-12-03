package org.eclipse.ecf.provider.jms.mqtt.container;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import javax.jms.JMSException;

import org.eclipse.ecf.core.util.Trace;
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

	public String getCorrelationId() {
		return this.correlationId;
	}

	public byte[] getData() {
		return data;
	}

	static void publish(final MqttClient client, final String topic,
			byte[] message, String jmsCorrelationId) throws JMSException {
		try {
			final MqttMessage m = new MqttMessage(getECFPayload(message,
					jmsCorrelationId));
			m.setRetained(false);
			new Thread(new Runnable() {
				public void run() {
					try {
						client.publish(topic, m);
					} catch (MqttException e) {
						// TODO Auto-generated catch block
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

	private static byte[] getECFPayload(byte[] message, String jmsCorrelationId)
			throws IOException {
		ByteArrayOutputStream bouts = new ByteArrayOutputStream();
		bouts.write(ECFPREFIX);
		ObjectOutputStream oos = new ObjectOutputStream(bouts);
		oos.writeObject(jmsCorrelationId);
		oos.writeObject(message);
		return bouts.toByteArray();
	}

	static MQTTMessage deserialize(byte[] bytes) {
		// Check the first four bytes
		if (!checkMessagePrefix(bytes))
			return null;
		// else it's an ECF message
		ByteArrayInputStream bins = new ByteArrayInputStream(bytes,
				ECFPREFIX.length, bytes.length - ECFPREFIX.length);
		ObjectInputStream oos;
		try {
			oos = new ObjectInputStream(bins);
			return new MQTTMessage((String) oos.readObject(),
					(byte[]) oos.readObject());
		} catch (IOException e) {
			Trace.throwing("org.eclipse.ecf.provider.jms.mqtt", "throwing",
					MQTTMessage.class, "deserialize", e);
			return null;
		} catch (ClassNotFoundException e) {
			Trace.throwing("org.eclipse.ecf.provider.jms.mqtt", "throwing",
					MQTTMessage.class, "deserialize", e);
			return null;
		}
	}

	private static boolean checkMessagePrefix(byte[] bytes) {
		for (int i = 0; i < ECFPREFIX.length; i++)
			if (ECFPREFIX[i] != bytes[i])
				return false;
		return true;
	}
}
