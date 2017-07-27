package daris.lifepool.client;

import java.io.File;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

public class DataUploadSettings extends ConnectionSettings {

    public static final String PROPERTY_PID = "pid";
    public static final String PROPERTY_CONTINUE_ON_ERROR = "upload.continue-on-error";
    public static final String PROPERTY_CSUM = "upload.csum";
    public static final String PROPERTY_PATIENT_ID_MAP = "upload.patient.id.map";
    public static final String PROPERTY_VERBOSE = "upload.verbose";
    public static final String PROPERTY_LOGGING = "upload.logging";

    private boolean _continueOnError;
    private boolean _csum;
    private File _patientIdMappingFile;
    private String _pid;
    private boolean _verbose;
    private boolean _logging;
    private Set<File> _files;

    public DataUploadSettings(Properties properties, File... files) {
        super(properties);
        _files = new LinkedHashSet<File>();
        if (files != null) {
            for (File file : files) {
                _files.add(file);
            }
        }
    }

    public DataUploadSettings() {
        this(null);
    }

    public void loadFromProperties(Properties properties) {
        super.loadFromProperties(properties);
        if (properties != null) {
            if (properties.containsKey(PROPERTY_PID)) {
                _pid = properties.getProperty(PROPERTY_PID);
            }
            if (properties.containsKey(PROPERTY_PATIENT_ID_MAP)) {
                setPatientIdMappingFile(properties.getProperty(PROPERTY_PATIENT_ID_MAP));
            }
            if (properties.containsKey(PROPERTY_CSUM)) {
                String csum = properties.getProperty(PROPERTY_CSUM);
                _csum = "1".equals(csum) || "true".equalsIgnoreCase(csum);
            }
            if (properties.containsKey(PROPERTY_CONTINUE_ON_ERROR)) {
                String continueOnError = properties.getProperty(PROPERTY_CONTINUE_ON_ERROR);
                _continueOnError = "1".equals(continueOnError) || "true".equalsIgnoreCase(continueOnError);
            }
            if (properties.containsKey(PROPERTY_VERBOSE)) {
                String verbose = properties.getProperty(PROPERTY_VERBOSE);
                _verbose = "1".equals(verbose) || "true".equalsIgnoreCase(verbose);
            }
            if (properties.containsKey(PROPERTY_LOGGING)) {
                String logging = properties.getProperty(PROPERTY_LOGGING);
                _logging = "1".equals(logging) || "true".equalsIgnoreCase(logging);
            }
        }
    }

    public boolean checkCSum() {
        return _csum;
    }

    public boolean continueOnError() {
        return _continueOnError;
    }

    public void setCheckCSum(boolean checkCSum) {
        _csum = checkCSum;
    }

    public void setContinueOnError(boolean continueOnError) {
        _continueOnError = continueOnError;
    }

    public void setPatientIdMappingFile(String patientIdMappingFilePath) {
        setPatientIdMappingFile(new File(patientIdMappingFilePath));
    }

    public void setPatientIdMappingFile(File patientIdMappingFile) {
        _patientIdMappingFile = patientIdMappingFile;
    }

    public File patientIdMappingFile() {
        return _patientIdMappingFile;
    }

    public void setVerbose(boolean verbose) {
        _verbose = verbose;
    }

    public boolean verbose() {
        return _verbose;
    }

    public void setLogging(boolean logging) {
        _logging = logging;
    }

    public boolean logging() {
        return _logging;
    }

    public String projectId() {
        return _pid;
    }

    public void setProjectId(String projectId) {
        _pid = projectId;
    }

    public Set<File> files() {
        return _files;
    }

    public void addFile(File file) {
        _files.add(file);
    }

    public void addFile(String path) throws Throwable {
        File file = new File(path);
        try {
            if (file.exists()) {
                addFile(file);
            } else {
                throw new IllegalArgumentException("Input file/directory: '" + path + "' does not exist.");
            }
        } catch (SecurityException se) {
            throw new IllegalArgumentException("Input file/directory: '" + path + "' is not accessible.", se);
        }
    }

    @Override
    public void validate() throws Throwable {
        super.validate();
        if (_pid == null) {
            throw new IllegalArgumentException("Missing pid");
        }
        if (_patientIdMappingFile == null) {
            throw new IllegalArgumentException("Missing patient.id.map");
        }
        if (!_patientIdMappingFile.exists()) {
            throw new IllegalArgumentException(
                    "patient.id.map file: '" + _patientIdMappingFile.getPath() + "' does not exist.");
        }
        if (_files.isEmpty()) {
            throw new IllegalArgumentException("Missing input dicom files or directories");
        }
    }

}
