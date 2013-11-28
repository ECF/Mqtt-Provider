<?xml version="1.0" encoding="UTF-8"?>
<md:mspec xmlns:md="http://www.eclipse.org/buckminster/MetaData-1.0" 
    name="org.eclipse.ecf.provider.jms.mqtt" 
    materializer="p2" 
    url="org.eclipse.ecf.provider.jms.mqtt.cquery">

    <md:mspecNode namePattern="^org\.eclipse\.ecf\.provider\.jms\.mqtt(\..+)?" materializer="workspace"/>
    <md:mspecNode namePattern="^org\.eclipse\.ecf\.provider\.jms\.mqtt\.feature?" materializer="workspace"/>

    <md:mspecNode namePattern="^org\.eclipse\.ecf\.tests\.provider\.jms?" materializer="workspace"/>
    <md:mspecNode namePattern="^org\.eclipse\.ecf\.tests\.provider\.jms\.mqtt(\..+)?" materializer="workspace"/>

    <md:mspecNode namePattern=".*" installLocation="${targetPlatformPath}"/>
</md:mspec>

