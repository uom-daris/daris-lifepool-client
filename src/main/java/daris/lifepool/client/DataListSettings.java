package daris.lifepool.client;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

public class DataListSettings extends ConnectionSettings {

    public static final String PROPERTY_PID = "pid";

    private Set<String> _accessionNumbers;
    private Set<String> _patientIds;

    private String _pid;

    public DataListSettings(Properties properties) {
        super(properties);
        _accessionNumbers = new TreeSet<String>();
        _patientIds = new TreeSet<String>();
    }

    public DataListSettings() {
        this(null);
    }

    public void addAccessionNumber(String accessionNumber) {
        _accessionNumbers.add(accessionNumber);
    }

    public Set<String> accesionNumbers() {
        return Collections.unmodifiableSet(_accessionNumbers);
    }

    public void addPatientId(String patientId) {
        _patientIds.add(patientId);
    }

    public Set<String> patientIds() {
        return Collections.unmodifiableSet(_patientIds);
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
        if (_accessionNumbers.isEmpty() && _patientIds.isEmpty()) {
            throw new IllegalArgumentException("Missing accession numbers or patient ids.");
        }
    }

}
