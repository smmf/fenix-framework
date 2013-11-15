/*
 * Fenix Framework, a framework to develop Java Enterprise Applications.
 *
 * Copyright (C) 2013 Fenix Framework Team and/or its affiliates and other contributors as indicated by the @author tags.
 *
 * This file is part of the Fenix Framework.  Read the file COPYRIGHT.TXT for more copyright and licensing information.
 */
package pt.ist.fenixframework.backend.jvstm.lf;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.ist.fenixframework.backend.jvstm.JVSTMConfig;
import pt.ist.fenixframework.backend.jvstm.comms.hazelcast.HazelcastCommSystem;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.TopicConfig;

/**
 * This is the configuration manager used by the fenix-framework-backend-jvstm-lf-cluster project.
 * 
 * @see Config
 */
public class JvstmLockFreeConfig extends JVSTMConfig {
    private static final Logger logger = LoggerFactory.getLogger(JvstmLockFreeConfig.class);

    protected static final String HAZELCAST_FF_GROUP_NAME = "FenixFrameworkGroup";

    public static final String DATAGRID_PARAM_PREFIX = "dataGrid.";

    /**
     * This <strong>optional</strong> parameter specifies the Hazelcast configuration file. This
     * configuration will used to create a group communication system between Fenix Framework nodes. The default value
     * for this parameter is <code>fenix-framework-hazelcast-default.xml</code>, which is the default
     * configuration file that ships with the framework.
     */
    protected String hazelcastConfigFile = "fenix-framework-lf-hazelcast-default.xml";

    /**
     * This <strong>optional</strong> parameter specifies the group communication system to use to broadcast the commit requests.
     * The value should be the name of a class that implements
     * pt.ist.fenixframework.backend.jvstm.comms.zmq.CommSystem. It defaults to
     * pt.ist.fenixframework.backend.jvstm.comms.hazelcast.HazelcastCommSystem.
     */
    protected String commSystemClassName = HazelcastCommSystem.class.getCanonicalName();

    public String getHazelcastConfigFile() {
        return this.hazelcastConfigFile;
    }

    public String getCommSystemClassName() {
        return this.commSystemClassName;
    }

    /**
     * This <strong>required</strong> parameter specifies the classname of the datagrid implementation.
     */
    protected String dataGridClassName = null;

    /**
     * This {@link Map} contains datagrid-specific properties. Any property found in the FF configuration starting with
     * <code>DATAGRID_PARAM_PREFIX</code> will be stored in this map (striped of the prefix). It is up to the concrete datagrid
     * implementation to make sense of these properties.
     */
    protected HashMap<String, String> dataGridPropertiesMap = new HashMap<String, String>();

    public String getDatagridClassName() {
        return this.dataGridClassName;
    }

    public String getDataGridProperty(String propName) {
        return this.dataGridPropertiesMap.get(propName);
    }

    private void setDataGridProperty(String propName, String value) {
        this.dataGridPropertiesMap.put(propName, value);
    }

    @Override
    protected void init() {
        JvstmLockFreeBackEnd thisBackEnd = new JvstmLockFreeBackEnd();
        super.backEnd = thisBackEnd;

        this.backEnd.getRepository().initBare(this);

        /* By this point we should already have done the minimum repository
        initialization, but not have written anything to it yet.   In case of a
        distributed data grid, this will enable the nodes to see each other
        before any updates begin to occur.  The following call will commence to
        update data in the repository. */

        super.init(); // this will in turn initialize our backend
    }

    @Override
    protected void setProperty(String propName, String value) {
        if (propName.startsWith(DATAGRID_PARAM_PREFIX)) {
            logger.info("Intercepting datagrid-specific property: {}={}", propName, value);
            setDataGridProperty(propName.substring(DATAGRID_PARAM_PREFIX.length()), value);
        } else {
            super.setProperty(propName, value);
        }
    }

    @Override
    protected void checkConfig() {
        super.checkConfig();
        checkRequired(this.dataGridClassName, "dataGridClassName");
    }

    @Override
    public JvstmLockFreeBackEnd getBackEnd() {
        return (JvstmLockFreeBackEnd) this.backEnd;
    }

    public com.hazelcast.config.Config getHazelcastConfig() {
        System.setProperty("hazelcast.logging.type", "slf4j");
        com.hazelcast.config.Config hzlCfg = new ClasspathXmlConfig(getHazelcastConfigFile());
        hzlCfg.getGroupConfig().setName(HAZELCAST_FF_GROUP_NAME);

        // turn on global ordering for the commit topic
        TopicConfig topicConfig = hzlCfg.getTopicConfig(LockFreeClusterUtils.FF_COMMIT_TOPIC_NAME);
        topicConfig.setGlobalOrderingEnabled(true);
        hzlCfg.addTopicConfig(topicConfig);

        topicConfig = hzlCfg.getTopicConfig(LockFreeClusterUtils.FF_COMMIT_TOPIC_NAME);

        return hzlCfg;
    }

}
