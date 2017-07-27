package daris.lifepool.client.dicom;

import java.io.File;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.pixelmed.dicom.AttributeList;
import com.pixelmed.dicom.TransferSyntax;

import arc.archive.ArchiveOutput;
import arc.archive.ArchiveRegistry;
import arc.mf.client.ServerClient;
import arc.mf.client.archive.Archive;
import arc.streams.StreamCopy.AbortCheck;
import arc.xml.XmlDoc;
import arc.xml.XmlStringWriter;
import arc.xml.XmlWriter;

public class DicomIngest {

    public static class Settings {

        public static final int DEFAULT_COMPRESSION_LEVEL = 6;

        private Map<String, String> _args;
        private Boolean _anonymize;
        private String _engine;
        private String _service;
        private String _type; // mime type of input stream
        private int _compressionLevel = DEFAULT_COMPRESSION_LEVEL;

        public String mimeTypeOfInputStream() {
            return _type;
        }

        public Settings setMimeTypeOfInputStream(String type) {
            _type = type;
            return this;
        }

        public String postfixService() {
            return _service;
        }

        public Settings setPostfixService(String service) {
            _service = service;
            return this;
        }

        public Boolean anonymize() {
            return _anonymize;
        }

        public Settings setAnonymize(Boolean anonymize) {
            _anonymize = anonymize;
            return this;
        }

        public String engine() {
            return _engine;
        }

        public Settings setEngine(String engine) {
            _engine = engine;
            return this;
        }

        public Map<String, String> args() {
            return _args;
        }

        public Settings setArgs(Map<String, String> args) {
            _args = args;
            return this;
        }

        public Settings setArg(String name, String value) {
            if (_args == null) {
                _args = new LinkedHashMap<String, String>();
            }
            _args.put(name, value);
            return this;
        }

        public int compressionLevel() {
            return _compressionLevel;
        }

        public Settings setCompressionLevel(int compressionLevel) {
            if (compressionLevel < 0) {
                _compressionLevel = 0;
            } else if (compressionLevel > 9) {
                _compressionLevel = 9;
            } else {
                _compressionLevel = compressionLevel;
            }
            return this;
        }

        public void save(XmlWriter w) throws Throwable {
            w.add("engine", _engine);
            if (_anonymize != null) {
                w.add("anonymize", _anonymize);
            }
            if (_service != null) {
                w.add("service", _service);
            }
            if (_type != null) {
                w.add("type", _type);
            }
            w.add("wait", true);
            if (_args != null) {
                Set<String> names = _args.keySet();
                for (String name : names) {
                    w.add("arg", new String[] { "name", name }, _args.get(name));
                }
            }
        }
    }

    public static String ingest(ServerClient.Connection cxn, AttributeList attributeList, String sourcePath,
            String projectCid, Logger logger) throws Throwable {
        Settings settings = new Settings();
        settings.setEngine("nig.dicom");
        settings.setAnonymize(true);
        settings.setMimeTypeOfInputStream("application/arc-archive");
        settings.setArg("nig.dicom.id.by",
                "study.id,patient.name.first,patient.name,patient.id,referring.physician.name");
        settings.setArg("nig.dicom.id.ignore-non-digits", "true");
        settings.setArg("nig.dicom.subject.create", "true");
        settings.setArg("nig.dicom.subject.find.method", "id");
        settings.setArg("nig.dicom.subject.meta.set-service", "vicnode.daris.subject.meta.set");
        settings.setArg("nig.dicom.subject.name.from.id", "true");
        settings.setArg("nig.dicom.write.mf-dicom-patient", "true");
        settings.setArg("nig.dicom.id.citable", projectCid);
        return ingest(cxn, attributeList, sourcePath, settings, logger);
    }

    public static String ingest(ServerClient.Connection cxn, File dicomFile, String projectCid) throws Throwable {
        Settings settings = new Settings();
        settings.setEngine("nig.dicom");
        settings.setAnonymize(true);
        settings.setMimeTypeOfInputStream("application/arc-archive");
        settings.setArg("nig.dicom.id.by",
                "study.id,patient.name.first,patient.name,patient.id,referring.physician.name");
        settings.setArg("nig.dicom.id.ignore-non-digits", "true");
        settings.setArg("nig.dicom.subject.create", "true");
        settings.setArg("nig.dicom.subject.find.method", "id");
        settings.setArg("nig.dicom.subject.meta.set-service", "vicnode.daris.subject.meta.set");
        settings.setArg("nig.dicom.subject.name.from.id", "true");
        settings.setArg("nig.dicom.write.mf-dicom-patient", "true");
        settings.setArg("nig.dicom.id.citable", projectCid);
        return ingest(cxn, dicomFile, settings);
    }

    public static String ingest(ServerClient.Connection cxn, File dicomFile, Settings settings) throws Throwable {
        List<File> dicomFiles = new ArrayList<File>(1);
        dicomFiles.add(dicomFile);
        XmlDoc.Element re = ingest(cxn, dicomFiles, settings);
        String studyAssetId = re.value("study/@id");
        String studyCid = cxn.execute("asset.identifier.get", "<id>" + studyAssetId + "</id>").value("id/@cid");
        return studyCid;
    }

    public static XmlDoc.Element ingest(ServerClient.Connection cxn, final List<File> dicomFiles,
            final Settings settings) throws Throwable {

        XmlStringWriter w = new XmlStringWriter();
        settings.save(w);

        Archive.declareSupportForAllTypes();
        Collections.sort(dicomFiles);
        ServerClient.Input sci = new ServerClient.GeneratedInput("application/arc-archive", "aar",
                dicomFiles.get(0).getParentFile().getAbsolutePath(), -1, null) {

            @Override
            protected void copyTo(OutputStream os, AbortCheck ac) throws Throwable {
                ArchiveOutput ao = ArchiveRegistry.createOutput(os, "application/arc-archive",
                        settings.compressionLevel(), null);
                try {
                    for (int i = 0; i < dicomFiles.size(); i++) {
                        File dicomFile = dicomFiles.get(i);
                        ao.add("application/dicom", String.format("%08d.dcm", i + 1), dicomFile);
                    }
                } finally {
                    ao.close();
                }

            }
        };
        return cxn.execute("dicom.ingest", w.document(), sci);
    }

    public static String ingest(ServerClient.Connection cxn, final AttributeList attributeList, final String sourcePath,
            final Settings settings, final Logger logger) throws Throwable {

        XmlStringWriter w = new XmlStringWriter();
        settings.save(w);

        Archive.declareSupportForAllTypes();
        ServerClient.Input sci = new ServerClient.GeneratedInput("application/arc-archive", "aar", sourcePath, -1,
                null) {

            @Override
            protected void copyTo(OutputStream os, AbortCheck ac) throws Throwable {
                ArchiveOutput ao = ArchiveRegistry.createOutput(os, "application/arc-archive",
                        settings.compressionLevel(), null);
                PipedInputStream pis = new PipedInputStream();
                PipedOutputStream pos = new PipedOutputStream(pis);
                Throwable[] workerException = new Throwable[1];
                Thread workerThread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            try {
                                attributeList.write(pos, TransferSyntax.ExplicitVRLittleEndian, true, true);
                            } finally {
                                pos.close();
                            }
                        } catch (Throwable e) {
                            if(logger!=null){
                                logger.log(Level.SEVERE, e.getMessage(), e);
                            }
                            System.err.println("Error in worker thread: " + Thread.currentThread().getName());
                            e.printStackTrace(System.err);
                            workerException[0] = e;
                        }
                    }
                }, "Writing DICOM to PipedOutputStream");
                try {
                    workerThread.start();
                    ao.add("application/dicom", Paths.get(sourcePath).getFileName().toString(), pis, -1);
                    workerThread.join();
                    if (workerException[0] != null) {
                        throw new Exception("Worker thread exception: " + workerException[0].getMessage(),
                                workerException[0]);
                    }
                } finally {
                    ao.close();
                }
            }
        };
        XmlDoc.Element re = cxn.execute("dicom.ingest", w.document(), sci);
        String studyAssetId = re.value("study/@id");
        String studyCid = cxn.execute("asset.identifier.get", "<id>" + studyAssetId + "</id>").value("id/@cid");
        return studyCid;
    }

}
