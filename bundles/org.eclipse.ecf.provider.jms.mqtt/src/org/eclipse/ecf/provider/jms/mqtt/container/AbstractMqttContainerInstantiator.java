/*******************************************************************************
 * Copyright (c) 2015 Composent, Inc. and others. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Composent, Inc. - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.provider.jms.mqtt.container;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.ecf.core.ContainerCreateException;
import org.eclipse.ecf.core.ContainerTypeDescription;
import org.eclipse.ecf.core.IContainer;
import org.eclipse.ecf.core.identity.ID;
import org.eclipse.ecf.provider.jms.container.AbstractJMSContainerInstantiator;
import org.eclipse.ecf.provider.jms.container.JMSContainerConfig;
import org.eclipse.ecf.provider.jms.identity.JMSID;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

public abstract class AbstractMqttContainerInstantiator extends AbstractJMSContainerInstantiator {

	public static final String[] mqttIntents = { "MQTT" };

	public static final String MQTTCONNECTOPTIONS_PARAM = "mqttconnectoptions";

	public AbstractMqttContainerInstantiator(String exporter, String importer) {
		super(exporter, importer);
	}

	protected abstract IContainer createMqttContainer(JMSContainerConfig config, MqttConnectOptions options,
			Map<String, ?> parameters) throws Exception;

	@SuppressWarnings("unchecked")
	@Override
	public IContainer createInstance(ContainerTypeDescription description, Object[] parameters)
			throws ContainerCreateException {
		String id = null;
		if (parameters != null) {
			if (parameters[0] instanceof Map)
				return createInstance(description, (Map<String,?>) parameters[0]);
			else	
			for (int i = 0; i < parameters.length; i++)
				if (parameters[i] instanceof String)
					id = (String) parameters[i];
				else if (parameters[i] instanceof ID)
					id = ((ID) parameters[i]).getName();
		}
		@SuppressWarnings("rawtypes")
		Map params = new HashMap();
		if (id != null)
			params.put(ID_PARAM, id);
		return super.createInstance(description, new Object[] { params });
	}

	protected abstract JMSID createContainerID(Map<String, ?> parameters) throws Exception;

	@Override
	public IContainer createInstance(ContainerTypeDescription description, Map<String, ?> parameters)
			throws ContainerCreateException {
		try {
			return createMqttContainer(
					new JMSContainerConfig(createContainerID(parameters),
							getKeepAlive(parameters, new Integer(MqttJMSServerContainer.DEFAULT_KEEPALIVE))),
					getParameterValue(parameters, MQTTCONNECTOPTIONS_PARAM, MqttConnectOptions.class,
							new MqttConnectOptions()),
					parameters);
		} catch (Exception e) {
			return throwCreateException("Could not create mqtt container", e);
		}
	}

	public String[] getSupportedIntents(ContainerTypeDescription description) {
		List<String> results = new ArrayList<String>();
		String[] genericIntents = super.getSupportedIntents(description);
		for (int i = 0; i < genericIntents.length; i++)
			results.add(genericIntents[i]);
		for (int i = 0; i < mqttIntents.length; i++)
			results.add(mqttIntents[i]);
		return (String[]) results.toArray(new String[] {});
	}

}
