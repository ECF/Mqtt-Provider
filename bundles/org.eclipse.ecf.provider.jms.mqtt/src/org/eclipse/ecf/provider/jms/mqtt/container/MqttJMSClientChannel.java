package org.eclipse.ecf.provider.jms.mqtt.container;

import java.io.IOException;
import java.io.Serializable;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

import org.eclipse.ecf.core.util.ECFException;
import org.eclipse.ecf.core.util.Trace;
import org.eclipse.ecf.provider.comm.ConnectionEvent;
import org.eclipse.ecf.provider.comm.ISynchAsynchEventHandler;
import org.eclipse.ecf.provider.jms.channel.AbstractJMSClientChannel;
import org.eclipse.ecf.provider.jms.identity.JMSID;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MqttJMSClientChannel extends AbstractJMSClientChannel {

	private static final long serialVersionUID = -4250141332659030158L;

	private static final int DEFAULT_KEEPALIVE = 20000;

	private String mqttClientId;
	private MqttConnectOptions mqttConnectOptions;
	private MqttClient mqttClient;
	private JMSID targetID;

	private MqttCallback callback = new MqttCallback() {
		public void connectionLost(Throwable cause) {
		}

		public void messageArrived(String topic, MqttMessage message)
				throws Exception {
			handleMessageArrived(topic, message);
		}

		public void deliveryComplete(IMqttDeliveryToken token) {
		}
	};

	public MqttJMSClientChannel(ISynchAsynchEventHandler handler,
			String mqttClientId, MqttConnectOptions options) {
		super(handler, (options == null) ? DEFAULT_KEEPALIVE : options
				.getKeepAliveInterval());
		this.mqttClientId = (mqttClientId == null) ? MqttClient
				.generateClientId() : mqttClientId;
		this.mqttConnectOptions = options;
	}

	@Override
	public boolean isConnected() {
		return (this.mqttClient != null);
	}

	@Override
	protected Serializable setupJMS(JMSID targetID, Object data)
			throws ECFException {
		// Create mqttClient
		try {
			if (!(data instanceof Serializable))
				throw new ECFException("connect data=" + data
						+ " must be Serializable");
			this.mqttClient = new MqttClient(targetID.getBroker(),
					this.mqttClientId);
			// Set callback
			this.mqttClient.setCallback(callback);
			// Connect to broker with connectOpts
			if (this.mqttConnectOptions == null)
				this.mqttClient.connect();
			else
				this.mqttClient.connect(this.mqttConnectOptions);
			// Subscribe to topic
			this.targetID = targetID;
			this.mqttClient.subscribe(this.targetID.getTopicOrQueueName());
			return (Serializable) data;
		} catch (MqttException e) {
			throw new ECFException("Could not connect to targetID="
					+ targetID.getName());
		}
	}

	@Override
	protected void createAndSendMessage(Serializable object,
			String jmsCorrelationId) throws JMSException {
		MQTTMessage.publish(this.mqttClient,
				this.targetID.getTopicOrQueueName(), object, jmsCorrelationId);
	}

	protected Object readObject(byte[] bytes) throws IOException,
			ClassNotFoundException {
		return MQTTMessage.deserialize(bytes);
	}

	public void disconnect() {
		if (this.mqttClient != null) {
			try {
				this.mqttClient.disconnect();
				this.mqttClient.close();
			} catch (MqttException e) {
				// Should not occur
				e.printStackTrace();
			}
			this.mqttClient = null;
			this.targetID = null;
			this.mqttConnectOptions = null;
		}
		synchronized (this.waitResponse) {
			waitResponse.notifyAll();
		}
		fireListenersDisconnect(new ConnectionEvent(this, null));
		connectionListeners.clear();
	}

	void handleMessageArrived(String topic, MqttMessage message) {
		Trace.trace("org.eclipse.ecf.provider.jms.mqtt",
				"handleMessageArrived topic=" + topic + ", message=" + message);
		// First, verify that the topic is ours...otherwise, ignore
		if (targetID == null) {
			Trace.trace("org.eclipse.ecf.provider.jms.mqtt",
					"handleMessageArrived.  targetID is null, so not processing");
			return;
		}
		String localTopic = targetID.getTopicOrQueueName();
		if (localTopic == null || !localTopic.equals(topic)) {
			Trace.trace("org.eclipse.ecf.provider.jms.mqtt",
					"handleMessageArrived.  Our topic=" + localTopic
							+ " message topic=" + topic);
			return;
		}
		// Just log all persistent messages and return
		if (message.isRetained()) {
			Trace.trace("org.eclipse.ecf.provider.jms.mqtt",
					"handleMessageArrived.  Message=" + message
							+ " retained, so not processing");
			return;
		}
		if (message.isDuplicate()) {
			Trace.trace("org.eclipse.ecf.provider.jms.mqtt",
					"handleMessageArrived.  Message=" + message
							+ " is duplicate, so not processing");
			return;
		}
		byte[] messageBytes = message.getPayload();
		MQTTMessage m = null;
		try {
			m = (MQTTMessage) readObject(messageBytes);
		} catch (Exception e) {
			Trace.throwing("org.eclipse.ecf.provider.jms.mqtt", "throwing",
					this.getClass(), "handleMessageArrived", e);
			return;
		}
		handleMessage(m.getData(), m.getJMSCorrelationId());
	}

	@Override
	protected ConnectionFactory createJMSConnectionFactory(JMSID targetID)
			throws IOException {
		return null;
	}

}
