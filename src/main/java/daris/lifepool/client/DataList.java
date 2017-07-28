package daris.lifepool.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import arc.mf.client.ServerClient;
import arc.xml.XmlDoc;
import arc.xml.XmlStringWriter;
import daris.lifepool.client.DataList.ResultEntry;
import daris.lifepool.client.task.Task;
import daris.util.CiteableIdUtils;

public class DataList extends Task<Set<ResultEntry>> {

    public static class ResultEntry implements Comparable<ResultEntry> {

        private String _cid;
        private String _sopInstanceUID;
        private String _accessionNumber;
        private String _patientId;

        public ResultEntry(XmlDoc.Element xe) throws Throwable {
            _cid = xe.value("cid");
            _sopInstanceUID = xe.value("sop-instance-uid");
            _accessionNumber = xe.value("accession-number");
        }

        public String accessionNumber() {
            return _accessionNumber;
        }

        public String sopInstanceUID() {
            return _sopInstanceUID;
        }

        public String cid() {
            return _cid;
        }

        public String fileName() {
            if (_sopInstanceUID != null) {
                return _sopInstanceUID + ".dcm";
            }
            return null;
        }

        public String patientId() {
            return _patientId;
        }

        void setPatientId(String patientId) {
            _patientId = patientId;
        }

        @Override
        public boolean equals(Object o) {
            if (o != null && (o instanceof ResultEntry)) {
                ResultEntry re = (ResultEntry) o;
                return re.cid().equals(cid());
            }
            return false;
        }

        @Override
        public int compareTo(ResultEntry o) {
            if (patientId() != null && o.patientId() == null) {
                return 1;
            }
            if (patientId() == null && o.patientId() != null) {
                return -1;
            }
            int r = patientId().compareTo(o.patientId());
            if (r != 0) {
                return r;
            }
            if (accessionNumber() != null && o.accessionNumber() == null) {
                return 1;
            }
            if (accessionNumber() == null && o.accessionNumber() == null) {
                return 0;
            }
            if (accessionNumber() == null && o.accessionNumber() != null) {
                return -1;
            }
            r = accessionNumber().compareTo(o.accessionNumber());
            if (r != 0) {
                return r;
            }
            return CiteableIdUtils.compare(cid(), o.cid());
        }
    }

    private DataListSettings _settings;

    public DataList(DataListSettings settings) throws Throwable {
        _settings = settings;
        _settings.setApp(DataUpload.APP);
    }

    @Override
    public Set<ResultEntry> call() throws Exception {
        try {
           return execute();
        } catch (Throwable e) {
            if (e instanceof Exception) {
                if (e instanceof InterruptedException) {
                    if (!Thread.interrupted()) {
                        Thread.currentThread().interrupt();
                    }
                }
                throw (Exception) e;
            } else {
                throw new Exception(e);
            }
        }
    }

    protected Set<ResultEntry> execute() throws Throwable {
        ServerClient.Connection cxn = connect(_settings);
        try {
            Set<String> patientIds = _settings.patientIds();
            Set<String> accessionNumbers = _settings.accesionNumbers();
            Map<String, String> patientIdMap = new HashMap<String, String>();
            Set<ResultEntry> res = new TreeSet<ResultEntry>();
            for (String patientId : patientIds) {
                getDatasets(_settings.projectId(), patientId, patientIdMap, cxn, res);
            }
            for (String accessionNumber : accessionNumbers) {
                getDatasets(cxn, _settings.projectId(), accessionNumber, patientIdMap, res);
            }
            if (res.isEmpty()) {
                return null;
            }
            return res;
        } finally {
            cxn.closeAndDiscard();
        }
    }

    private static void getDatasets(String projectId, String patientId, Map<String, String> patientIdMap,
            ServerClient.Connection cxn, Set<ResultEntry> res) throws Throwable {

        StringBuilder sb = new StringBuilder();
        sb.append("cid starts with '").append(projectId).append("' and cid contained by (cid in '").append(projectId)
                .append("' and xpath(mf-dicom-patient/id)='").append(patientId)
                .append("') and daris:dicom-dataset has value");
        getDatasets(cxn, sb.toString(), patientIdMap, res);
    }

    private static void getDatasets(ServerClient.Connection cxn, String pid, String accessionNumber,
            Map<String, String> patientIdMap, Set<ResultEntry> res) throws Throwable {

        StringBuilder sb = new StringBuilder();
        sb.append("cid starts with '").append(pid)
                .append("' and xpath(daris:dicom-dataset/object/de[@tag='00080050']/value)='").append(accessionNumber)
                .append("'");
        getDatasets(cxn, sb.toString(), patientIdMap, res);
    }

    private static void getDatasets(ServerClient.Connection cxn, String where, Map<String, String> patientIdMap,
            Set<ResultEntry> res) throws Throwable {

        XmlStringWriter w = new XmlStringWriter();
        w.add("where", where);
        w.add("action", "get-value");
        w.add("size", "infinity");
        w.add("xpath", new String[] { "ename", "cid" }, "cid");
        w.add("xpath", new String[] { "ename", "accession-number" },
                "meta/daris:dicom-dataset/object/de[@tag='00080050']/value");
        w.add("xpath", new String[] { "ename", "sop-instance-uid" },
                "meta/daris:dicom-dataset/object/de[@tag='00080018']/value");
        List<XmlDoc.Element> aes = cxn.execute("asset.query", w.document()).elements("asset");
        if (aes != null && !aes.isEmpty()) {
            for (XmlDoc.Element ae : aes) {
                ResultEntry re = new ResultEntry(ae);
                String patientId = patientIdMap.get(re.accessionNumber());
                if (patientId != null) {
                    re.setPatientId(patientIdMap.get(re.accessionNumber()));
                } else {
                    String subjectCid = CiteableIdUtils.parent(re.cid(), 3);
                    patientId = cxn.execute("asset.get", "<cid>" + subjectCid + "</cid>")
                            .value("asset/meta/mf-dicom-patient/id");
                    re.setPatientId(patientId);
                    patientIdMap.put(re.accessionNumber(), patientId);
                }
                res.add(re);
            }
        }
    }

}
