/*******************************************************************************
 * Copyright (c) 2015 Composent, Inc. and others. All rights reserved. This
 * program and the accompanying materials are made available under the terms of
 * the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html
 * 
 * Contributors: Composent, Inc. - initial API and implementation
 ******************************************************************************/
package org.eclipse.ecf.provider.jms.mqtt.container;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.net.SocketFactory;

import org.eclipse.ecf.core.ContainerCreateException;
import org.eclipse.ecf.core.ContainerTypeDescription;
import org.eclipse.ecf.core.IContainer;
import org.eclipse.ecf.core.identity.ID;
import org.eclipse.ecf.core.provider.ContainerIntentException;
import org.eclipse.ecf.provider.jms.container.AbstractJMSContainerInstantiator;
import org.eclipse.ecf.provider.jms.container.JMSContainerConfig;
import org.eclipse.ecf.provider.jms.identity.JMSID;
import org.eclipse.ecf.remoteservice.Constants;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

public abstract class AbstractMqttContainerInstantiator extends AbstractJMSContainerInstantiator {

	public static final String[] mqttIntents = { "MQTT" };

	public static final String CO_P = "mqttconnectoptions";
	public static final String CLEANSESSION_P = "cleansession";
	public static final String CONNECTIONTIMEOUT_P = "connectiontimeout";
	public static final String KEEPALIVEINTERVAL_P = "keepaliveinterval";
	public static final String MAXINFLIGHT_P = "maxinflight";
	public static final String AUTORECONNECT_P = "autoreconnect";
	public static final String USERNAME_P = "username";
	public static final String PASSWORD_P = "password";
	public static final String SOCKETFACTORY_P = "socketfactory";
	public static final String SSLPROPERTIES_P = "sslproperties";
	public static final String CLIENTQOS_P = "clientqos";

	public AbstractMqttContainerInstantiator(String exporter, String importer) {
		super(exporter, importer);
	}

	public AbstractMqttContainerInstantiator(List<String> exporterConfigs,
			Map<String, List<String>> exporterConfigToImporterConfig) {
		super(exporterConfigs, exporterConfigToImporterConfig);
	}

	public AbstractMqttContainerInstantiator(String exporter, List<String> importers) {
		super(exporter, importers);
	}

	protected abstract IContainer createMqttContainer(JMSContainerConfig config, MqttConnectOptions options,
			int clientqos, Map<String, ?> parameters) throws Exception;

	@SuppressWarnings("unchecked")
	@Override
	public IContainer createInstance(ContainerTypeDescription description, Object[] parameters)
			throws ContainerCreateException {
		String id = null;
		if (parameters != null) {
			if (parameters[0] instanceof Map)
				return createInstance(description, (Map<String, ?>) parameters[0]);
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

	protected abstract JMSID createContainerID(ContainerTypeDescription description, Map<String, ?> parameters) throws Exception;

	private <T> T getMqttParameterValue(Map<String, ?> parameters, String param, Class<T> clazz, T def) {
		T result = getParameterValue(parameters, param, clazz, null);
		if (result != null) 
			return result;
		else {
			// If Clazz is String, then we just return default
			if (clazz.equals(String.class))
				return def;
			else {
				String strValue = getParameterValue(parameters, param, String.class,
						(def == null) ? null : def.toString());
				if (strValue != null) {
					try {
						if (clazz.equals(Integer.class) || clazz.equals(int.class))
							result = clazz.cast(Integer.valueOf(strValue));
						else if (clazz.equals(Double.class) || clazz.equals(double.class))
							result = clazz.cast(Double.valueOf(strValue));
						else if (clazz.equals(Short.class) || clazz.equals(short.class))
							result = clazz.cast(Short.valueOf(strValue));
						else if (clazz.equals(Long.class) || clazz.equals(long.class))
							result = clazz.cast(Long.valueOf(strValue));
						else if (clazz.equals(Boolean.class) || clazz.equals(boolean.class))
							result = clazz.cast(Boolean.valueOf(strValue));
					} catch (Exception e) {}
				}
				return def;
			}
		}
	}

	@Override
	protected boolean supportsOSGIAsyncIntent(ContainerTypeDescription description) {
		return true;
	}
	
	protected void checkJMSIDForIntents(ContainerTypeDescription description, JMSID id, Map<String,?> properties) throws ContainerIntentException {
		URI uri = null;
		try {
			uri = new URI(id.getName());
		} catch (URISyntaxException e) {
			throw new ContainerIntentException(Constants.OSGI_BASIC_INTENT, "Couldn't create URI from JMSID="+id.getName(),e);
		}
		checkOSGIIntents(description, uri, properties);
	}
	
	@Override
	public IContainer createInstance(ContainerTypeDescription description, Map<String, ?> parameters)
			throws ContainerCreateException {
		try {
			MqttConnectOptions options = getMqttParameterValue(parameters, CO_P, MqttConnectOptions.class,
					new MqttConnectOptions());
			Boolean b = getMqttParameterValue(parameters, CLEANSESSION_P, Boolean.class, Boolean.TRUE);
			options.setCleanSession(b);
			Integer ct = getMqttParameterValue(parameters, CONNECTIONTIMEOUT_P, Integer.class, null);
			if (ct != null)
				options.setKeepAliveInterval(ct);
			Boolean autoReconnect = getMqttParameterValue(parameters, AUTORECONNECT_P, Boolean.class, Boolean.TRUE);
			options.setAutomaticReconnect(autoReconnect);
			Integer ka = getMqttParameterValue(parameters, KEEPALIVEINTERVAL_P, Integer.class, null);
			if (ka != null)
				options.setKeepAliveInterval(ka);
			Integer mif = getMqttParameterValue(parameters, MAXINFLIGHT_P, Integer.class, null);
			if (mif != null)
				options.setMaxInflight(mif);
			String username = getMqttParameterValue(parameters, USERNAME_P, String.class, null);
			if (username != null) {
				String password = getMqttParameterValue(parameters, PASSWORD_P, String.class, null);
				if (password != null) {
					options.setUserName(username);
					options.setPassword(password.toCharArray());
				}
			}
			SocketFactory socketFactory = getMqttParameterValue(parameters, SOCKETFACTORY_P, SocketFactory.class, null);
			if (socketFactory != null)
				options.setSocketFactory(socketFactory);
			Properties properties = getMqttParameterValue(parameters, SSLPROPERTIES_P, Properties.class, null);
			if (properties != null)
				options.setSSLProperties(properties);
			Integer clientqos = getMqttParameterValue(parameters, CLIENTQOS_P, Integer.class, 0);
			return createMqttContainer(
					new JMSContainerConfig(createContainerID(description, parameters),
							getKeepAlive(parameters, new Integer(MqttJMSServerContainer.DEFAULT_KEEPALIVE))),
					options, clientqos, parameters);
		} catch (Exception e) {
			if (e instanceof ContainerIntentException) 
				throw (ContainerIntentException) e;
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
