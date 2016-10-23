package org.eclipse.ecf.provider.jms.mqtt;

import org.eclipse.ecf.provider.jms.mqtt.container.MqttJMSClientContainer;
import org.eclipse.ecf.provider.jms.mqtt.container.MqttJMSServerContainer;
import org.eclipse.ecf.provider.remoteservice.generic.RemoteServiceContainerAdapterFactory;
import org.eclipse.ecf.remoteservice.provider.AdapterConfig;
import org.eclipse.ecf.remoteservice.provider.IRemoteServiceDistributionProvider;
import org.eclipse.ecf.remoteservice.provider.RemoteServiceDistributionProvider;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

public class Activator implements BundleActivator {

	public void start(final BundleContext context) throws Exception {
		// Build and register mqtt manager distribution provider
		context.registerService(IRemoteServiceDistributionProvider.class,
				new RemoteServiceDistributionProvider.Builder().setName(MqttJMSServerContainer.MQTT_MANAGER_NAME)
						.setInstantiator(new MqttJMSServerContainer.Instantiator()).setDescription("ECF MQTT Manager")
						.setServer(true).setAdapterConfig(new AdapterConfig(new RemoteServiceContainerAdapterFactory(),
								MqttJMSServerContainer.class))
						.build(),
				null);
		// same with client
		context.registerService(IRemoteServiceDistributionProvider.class,
				new RemoteServiceDistributionProvider.Builder().setName(MqttJMSClientContainer.MQTT_CLIENT_NAME)
						.setInstantiator(new MqttJMSClientContainer.Instantiator()).setDescription("ECF MQTT Client")
						.setServer(true).setAdapterConfig(new AdapterConfig(new RemoteServiceContainerAdapterFactory(),
								MqttJMSClientContainer.class))
						.build(),
				null);
	}

	public void stop(BundleContext context) throws Exception {
	}

}
