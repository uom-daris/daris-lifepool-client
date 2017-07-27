package daris.lifepool.client;

import java.util.ArrayList;
import java.util.List;

import arc.mf.client.ServerClient;
import arc.xml.XmlDoc;
import arc.xml.XmlStringWriter;
import daris.lifepool.client.DataList.ResultEntry;
import daris.lifepool.client.task.Task;

public class DataList extends Task<List<ResultEntry>> {

    public static class ResultEntry {

        private String _cid;
        private String _sopInstanceUID;
        private String _accessionNumber;

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
    }

    private DataListSettings _settings;

    public DataList(DataListSettings settings) throws Throwable {
        _settings = settings;
        _settings.setApp(DataUpload.APP);
    }

    @Override
    public List<ResultEntry> call() throws Exception {
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

    protected List<ResultEntry> execute() throws Throwable {
        ServerClient.Connection cxn = connect(_settings);
        try {
            List<String> accessionNumbers = _settings.accesionNumbers();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < accessionNumbers.size(); i++) {
                String accessionNumber = accessionNumbers.get(i);
                if (i > 0) {
                    sb.append(" or ");
                }
                sb.append("(xpath(daris:dicom-dataset/object/de[@tag='00080050']/value)='").append(accessionNumber)
                        .append("')");
            }
            XmlStringWriter w = new XmlStringWriter();
            w.add("where", sb.toString());
            w.add("action", "get-value");
            w.add("size", "infinity");
            w.add("xpath", new String[] { "ename", "cid" }, "cid");
            w.add("xpath", new String[] { "ename", "accession-number" },
                    "meta/daris:dicom-dataset/object/de[@tag='00080050']/value");
            w.add("xpath", new String[] { "ename", "sop-instance-uid" },
                    "meta/daris:dicom-dataset/object/de[@tag='00080018']/value");
            XmlDoc.Element re = cxn.execute("asset.query", w.document());
            if (!re.elementExists("asset")) {
                return null;
            }
            List<XmlDoc.Element> aes = re.elements("asset");
            List<ResultEntry> res = new ArrayList<ResultEntry>(aes.size());
            for (XmlDoc.Element ae : aes) {
                res.add(new ResultEntry(ae));
            }
            return res;
        } finally {
            cxn.closeAndDiscard();
        }
    }
}
