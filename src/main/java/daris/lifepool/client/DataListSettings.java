package daris.lifepool.client;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class DataListSettings extends ConnectionSettings {

    public static final String PROPERTY_PID = "pid";

    private Set<String> _accessionNumbers;

    private String _pid;

    public DataListSettings(Properties properties) {
        super(properties);
        _accessionNumbers = new LinkedHashSet<String>();
    }

    public DataListSettings() {
        this(null);
    }

    public void addAccessionNumber(String accessionNumber) {
        _accessionNumbers.add(accessionNumber);
    }

    public List<String> accesionNumbers() {
        return new ArrayList<String>(_accessionNumbers);
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

    public void validate() throws Throwable {
        super.validate();
        if (_accessionNumbers.isEmpty()) {
            throw new IllegalArgumentException("Missing accession numbers.");
        }
    }

}
