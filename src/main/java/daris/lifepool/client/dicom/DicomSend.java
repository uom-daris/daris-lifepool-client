package daris.lifepool.client.dicom;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

import com.pixelmed.dicom.AttributeTag;
import com.pixelmed.dicom.SetOfDicomFiles;
import com.pixelmed.network.MultipleInstanceTransferStatusHandler;
import com.pixelmed.network.StorageSOPClassSCU;

public class DicomSend {

    public static class Settings {

        private String _host;
        private int _port;
        private String _calledAET;
        private String _callingAET;
        private Map<AttributeTag, String> _attributeValues;

        public Settings() {
            this(null, 104, null, null, null);
        }

        public Settings(String calledHost, int calledPort, String calledAET, String callingAET,
                Map<AttributeTag, String> attributeValues) {
            _host = calledHost;
            _port = calledPort;
            _calledAET = calledAET;
            _callingAET = callingAET;
            _attributeValues = attributeValues;
        }

        public String calledHost() {
            return _host;
        }

        public int calledPort() {
            return _port;
        }

        public String calledAET() {
            return _calledAET;
        }

        public String callingAET() {
            return _callingAET;
        }

        public Map<AttributeTag, String> attributeValues() {
            return _attributeValues;
        }

        public Settings setAttributeValue(AttributeTag tag, String value) {
            if (_attributeValues == null) {
                _attributeValues = new LinkedHashMap<AttributeTag, String>();
            }
            _attributeValues.put(tag, value);
            return this;
        }

        public Settings setAttributeValues(Map<AttributeTag, String> attributeValues) {
            _attributeValues = attributeValues;
            return this;
        }

        public Settings setCalledHost(String host) {
            _host = host;
            return this;
        }

        public Settings setCalledPort(int port) {
            _port = port;
            return this;
        }

        public Settings setCalledAET(String aet) {
            _calledAET = aet;
            return this;
        }

        public Settings setCallingAET(String aet) {
            _callingAET = aet;
            return this;
        }

        public boolean hasAttributeValues() {
            return _attributeValues != null && !_attributeValues.isEmpty();
        }

    }

    public static void send(SetOfDicomFiles dicomFiles, Settings settings) {
        final int total = dicomFiles.size();
        new StorageSOPClassSCU(settings.calledHost(), settings.calledPort(), settings.calledAET(),
                settings.callingAET(), dicomFiles, 0, new MultipleInstanceTransferStatusHandler() {
                    @Override
                    public void updateStatus(int nRemaining, int nCompleted, int nFailed, int nWarning,
                            String sopInstanceUID) {
                        if (total > 1) {
                            System.out.println("Sent " + nCompleted + " of " + total + " dicom files.");
                        }
                    }
                }, null, 0, 0);
    }

    public static void send(File dicomFile, Settings settings) throws Throwable {
        SetOfDicomFiles dicomFiles = new SetOfDicomFiles();
        dicomFiles.add(dicomFile);
        send(dicomFiles, settings);
    }

}
