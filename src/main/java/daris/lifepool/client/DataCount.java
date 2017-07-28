package daris.lifepool.client;

import arc.mf.client.ServerClient;
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

    @Override
    public ProjectSummary call() throws Exception {
        return null;
//        ServerClient.Connection cxn = connect(_settings);
//        try {
//            XmlStringWriter w = new XmlStringWriter();
//        } finally {
//            cxn.closeAndDiscard();
//        }
    }
    
    

}