package daris.lifepool.client;

import java.util.List;

import arc.mf.client.ServerClient;
import arc.xml.XmlDoc;
import arc.xml.XmlStringWriter;
import daris.lifepool.client.DataCount.ProjectSummary;
import daris.lifepool.client.task.Task;

public class DataCount extends Task<ProjectSummary> {

    public static class ProjectSummary {
        private long _nbAccessions;
        private long _nbPatients;
        private long _nbImages;
        private long _totalStorageUsage;

        public ProjectSummary(long nbPatients, long nbAccessions, long nbImages, long totalStorageUsage) {
            _nbPatients = nbPatients;
            _nbAccessions = nbAccessions;
            _nbImages = nbImages;
            _totalStorageUsage = totalStorageUsage;
        }

        public long numberOfAccessions() {
            return _nbAccessions;
        }

        public long numberofPatients() {
            return _nbPatients;
        }

        public long numberOfImages() {
            return _nbImages;
        }

        public long totalStorageUsage() {
            return _totalStorageUsage;
        }
    }

    private DataCountSettings _settings;

    public DataCount(DataCountSettings settings) {
        _settings = settings;
        _settings.setApp(DataUpload.APP);
    }

    @Override
    public ProjectSummary call() throws Exception {
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

    protected ProjectSummary execute() throws Throwable {
        ServerClient.Connection cxn = connect(_settings);
        try {
            XmlStringWriter w = new XmlStringWriter();
            w.push("service", new String[] { "name", "asset.query" });
            w.add("where", "cid in '" + _settings.projectId() + "'");
            w.add("action", "count");
            w.pop();

            w.push("service", new String[] { "name", "asset.query" });
            w.add("where", "cid starts with '" + _settings.projectId() + "' and model='om.pssd.study'");
            w.add("action", "count");
            w.pop();

            w.push("service", new String[] { "name", "asset.query" });
            w.add("where", "cid starts with '" + _settings.projectId()
                    + "' and model='om.pssd.dataset' and daris:dicom-dataset has value");
            w.add("action", "count");
            w.pop();

            w.push("service", new String[] { "name", "asset.query" });
            w.add("where", "cid starts with '" + _settings.projectId() + "' and asset has content");
            w.add("xpath", "content/size");
            w.add("action", "sum");
            w.pop();

            List<XmlDoc.Element> res = cxn.execute("service.execute", w.document()).elements("reply/response");
            long nbPatients = res.get(0).longValue("value");
            long nbAccessions = res.get(1).longValue("value");
            long nbImages = res.get(2).longValue("value");
            long totalStorageUsage = res.get(3).longValue("value");

            return new ProjectSummary(nbPatients, nbAccessions, nbImages, totalStorageUsage);
        } finally {
            cxn.closeAndDiscard();
        }
    }

}