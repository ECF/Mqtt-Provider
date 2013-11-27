package org.eclipse.ecf.provider.jms.mqtt.container;

import java.io.IOException;
import java.io.Serializable;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

import org.eclipse.ecf.core.identity.ID;
import org.eclipse.ecf.core.util.ECFException;
import org.eclipse.ecf.core.util.Trace;
import org.eclipse.ecf.provider.comm.ConnectionEvent;
import org.eclipse.ecf.provider.comm.ISynchAsynchEventHandler;
import org.eclipse.ecf.provider.jms.channel.AbstractJMSServerChannel;
import org.eclipse.ecf.provider.jms.identity.JMSID;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class MqttJMSServerChannel extends AbstractJMSServerChannel {

	private static final long serialVersionUID = -5177220940980707980L;

	private static final int DEFAULT_KEEPALIVE = 20000;

	private MqttConnectOptions mqttConnectOptions;
	private MqttClient mqttClient;
	private String mqttClientId;

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

	public MqttJMSServerChannel(ISynchAsynchEventHandler handler,
			String mqttClientId, MqttConnectOptions options)
			throws ECFException {
		super(handler, (options == null) ? DEFAULT_KEEPALIVE : options
				.getKeepAliveInterval());
		this.mqttConnectOptions = options;
		JMSID targetID = (JMSID) getLocalID();
		try {
			if (this.mqttClientId == null)
				this.mqttClientId = MqttClient.generateClientId();
			this.mqttClient = new MqttClient(targetID.getBroker(),
					MqttClient.generateClientId());
			// Set callback
			this.mqttClient.setCallback(callback);
			// Connect to broker with connectOpts
			if (this.mqttConnectOptions == null)
				this.mqttClient.connect();
			else
				this.mqttClient.connect(this.mqttConnectOptions);
			// Subscribe to topic
			this.mqttClient.subscribe(targetID.getTopicOrQueueName());
		} catch (MqttException e) {
			throw new ECFException("Could not connect to targetID"
					+ targetID.getName());
		}
	}

	@Override
	protected void createAndSendMessage(Serializable object,
			String jmsCorrelationId) throws JMSException {
		MQTTMessage.publish(this.mqttClient,
				((JMSID) getLocalID()).getTopicOrQueueName(), object,
				jmsCorrelationId);
	}

	public Client createClient(ID remoteID) {
		Client newclient = new Client(remoteID, false);
		newclient.start();
		return newclient;
	}

	@Override
	public boolean isConnected() {
		return (this.mqttClient != null);
	}

	@Override
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
			this.mqttConnectOptions = null;
		}
		synchronized (this.waitResponse) {
			waitResponse.notifyAll();
		}
		fireListenersDisconnect(new ConnectionEvent(this, null));
		connectionListeners.clear();
	}

	protected Object readObject(byte[] bytes) throws IOException,
			ClassNotFoundException {
		return MQTTMessage.deserialize(bytes);
	}

	void handleMessageArrived(String topic, MqttMessage message) {
		Trace.trace("org.eclipse.ecf.provider.jms.mqtt",
				"handleMessageArrived topic=" + topic + ", message=" + message);
		// First, verify that the topic is ours...otherwise, ignore
		String localTopic = ((JMSID) getLocalID()).getTopicOrQueueName();
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
