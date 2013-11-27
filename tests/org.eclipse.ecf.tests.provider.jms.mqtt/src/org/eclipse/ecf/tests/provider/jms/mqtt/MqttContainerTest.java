package org.eclipse.ecf.tests.provider.jms.mqtt;

import org.eclipse.ecf.core.ContainerFactory;
import org.eclipse.ecf.core.IContainer;
import org.eclipse.ecf.core.identity.ID;
import org.eclipse.ecf.core.identity.IDFactory;
import org.eclipse.ecf.tests.provider.jms.JMSContainerAbstractTestCase;

public class MqttContainerTest extends JMSContainerAbstractTestCase {

	@Override
	protected void setupBroker() throws Exception {
		// No broker
	}
	
	protected String getClientContainerName() {
		return Mqtt.CLIENT_CONTAINER_NAME;
	}

	/* (non-Javadoc)
	 * @see org.eclipse.ecf.tests.provider.jms.JMSContainerAbstractTestCase#getServerContainerName()
	 */
	protected String getServerContainerName() {
		return Mqtt.SERVER_CONTAINER_NAME;
	}

	/* (non-Javadoc)
	 * @see org.eclipse.ecf.tests.provider.jms.JMSContainerAbstractTestCase#getServerIdentity()
	 */
	protected String getServerIdentity() {
		return Mqtt.TARGET_NAME;
	}

	protected IContainer createServer() throws Exception {
		return ContainerFactory.getDefault().createContainer(
				getServerContainerName(), new Object[] { getServerIdentity() });
	}

	public void testConnectClient() throws Exception {
		IContainer client = getClients()[0];
		ID targetID = IDFactory.getDefault().createID(client.getConnectNamespace(),new Object [] { getServerIdentity() });
		Thread.sleep(3000);
		client.connect(targetID, null);
		Thread.sleep(3000);
	}

}
