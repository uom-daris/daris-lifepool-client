package daris.lifepool.client;

import java.util.Properties;

public class DataCountSettings extends ConnectionSettings {

    public static final String PROPERTY_PID = "pid";

    private String _pid;

    public DataCountSettings(Properties properties) {
        super(properties);
    }

    public DataCountSettings() {
        this(null);
    }

    public String projectId() {
        return _pid;
    }

    public void setProjectId(String pid) {
        _pid = pid;
    }

    public void loadFromProperties(Properties properties) {
        super.loadFromProperties(properties);
        if (properties != null) {
            if (properties.containsKey(PROPERTY_PID)) {
                _pid = properties.getProperty(PROPERTY_PID);
            }
        }
    }

}
