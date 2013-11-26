package org.eclipse.ecf.provider.jms.mqtt.container;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.UUID;

import javax.jms.JMSException;

import org.eclipse.ecf.provider.generic.SOContainer;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MQTTMessage implements Serializable {

	private static final long serialVersionUID = -6278911816004055002L;
	private byte[] data;
	private String jmsCorrelationId;

	public MQTTMessage(byte[] data) {
		this.data = data;
		this.jmsCorrelationId = UUID.randomUUID().toString();
	}

	public MQTTMessage(byte[] data, String jmsCorrelationId) {
		this.data = data;
		this.jmsCorrelationId = jmsCorrelationId;
	}

	public String getJMSCorrelationId() {
		return this.jmsCorrelationId;
	}

	public byte[] getData() {
		return data;
	}

	static void publish(final MqttClient client, final String topic,
			Serializable message, String jmsCorrelationId) throws JMSException {
		try {
			final MqttMessage m = new MqttMessage(
					SOContainer.serialize(new MQTTMessage(SOContainer
							.serialize(message), jmsCorrelationId)));
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

	static Object deserialize(byte[] bytes) throws IOException,
			ClassNotFoundException {
		return new ObjectInputStream(new ByteArrayInputStream(bytes))
				.readObject();
	}
}
