package daris.lifepool.client.upload;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.SimpleDateFormat;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Stream;

import com.pixelmed.dicom.Attribute;
import com.pixelmed.dicom.AttributeList;
import com.pixelmed.dicom.DicomFileUtilities;
import com.pixelmed.dicom.TagFromName;
import com.pixelmed.dicom.TransferSyntax;

import arc.archive.ArchiveOutput;
import arc.archive.ArchiveRegistry;
import arc.mf.client.ServerClient;
import arc.mf.client.ServerClient.Connection;
import arc.mf.client.archive.Archive;
import arc.streams.StreamCopy.AbortCheck;
import arc.xml.XmlDoc;
import arc.xml.XmlStringWriter;
import daris.dicom.util.DicomChecksumUtils;
import daris.lifepool.client.dicom.DicomIngest;
import daris.lifepool.client.dicom.DicomModify;
import daris.lifepool.client.task.Task;
import daris.util.CiteableIdUtils;
import daris.util.LoggingUtils;
import daris.util.ThrowableUtils;

public class DataUpload extends Task<Void> {

    public static final String APP = "daris-lifepool-data-upload";

    public static final String PROPERTIES_FILE = new StringBuilder()
            .append(System.getProperty("user.home").replace('\\', '/')).append("/.daris/").append(DataUpload.APP)
            .append(".properties").toString();

    public static final int INDENT = 4;

    public static DataUploadSettings loadSettingsFromPropertiesFile() {
        return DataUploadSettings.loadFromPropertiesFile(PROPERTIES_FILE);
    }

    static Logger getLogger() throws Throwable {
        Logger logger = LoggingUtils.createLogger(APP, Level.ALL, false);
        logger.addHandler(LoggingUtils.createFileHandler(APP));
        logger.addHandler(LoggingUtils.createStreamHandler(System.out, Level.ALL, new Formatter() {

            @Override
            public String format(LogRecord record) {
                StringBuilder sb = new StringBuilder();
                String message = record.getMessage();
                if (message != null) {
                    Level level = record.getLevel();
                    if (level.equals(Level.WARNING) || level.equals(Level.SEVERE)) {
                        while (message.startsWith(" ")) {
                            message = message.substring(1);
                            sb.append(" ");
                        }
                        sb.append(level.getName() + ": ");
                    }
                    sb.append(message);
                }
                sb.append("\n");
                Throwable error = record.getThrown();
                if (error != null) {
                    sb.append(ThrowableUtils.getStackTrace(error));
                    sb.append("\n");
                }
                return sb.toString();
            }
        }));
        return logger;
    }

    private DataUploadSettings _settings;

    public DataUpload(DataUploadSettings settings) throws Throwable {
        _settings = settings;
        _settings.setApp(APP);
        if (settings.logging()) {
            setLogger(getLogger());
        }
    }

    @Override
    public Void call() throws Exception {
        try {
            execute();
            return null;
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

    private void execute() throws Throwable {
        logInfo("loading (AccessionNumber -> PatientID) mapping from file: '"
                + _settings.patientIdMappingFile().getAbsolutePath() + "'");
        Map<String, String> patientIdMapping = loadPatientIdMapping(_settings.patientIdMappingFile());
        ServerClient.Connection cxn = connect(_settings);
        try {
            Set<File> inputs = _settings.files();
            for (File input : inputs) {
                if (Files.isDirectory(input.toPath())) {
                    Files.walkFileTree(input.toPath(), new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                            try {
                                uploadDicomFile(cxn, path.toFile(), patientIdMapping);
                            } catch (Throwable e) {
                                e.printStackTrace(System.err);
                                if (!_settings.continueOnError()) {
                                    return FileVisitResult.TERMINATE;
                                } else {
                                    return FileVisitResult.CONTINUE;
                                }
                            }
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFileFailed(Path path, IOException ioe) {
                            ioe.printStackTrace(System.err);
                            if (!_settings.continueOnError()) {
                                return FileVisitResult.TERMINATE;
                            } else {
                                return FileVisitResult.CONTINUE;
                            }
                        }
                    });
                } else {
                    try {
                        uploadDicomFile(cxn, input, patientIdMapping);
                    } catch (Throwable e) {
                        if (!_settings.continueOnError()) {
                            throw e;
                        } else {
                            logError(e.getMessage(), e);
                        }
                    }
                }
            }
        } finally {
            cxn.closeAndDiscard();
        }
    }

    private void uploadDicomFile(ServerClient.Connection cxn, File dicomFile, Map<String, String> patientIdMapping)
            throws Throwable {

        logInfo("Uploading file: '" + dicomFile.getAbsolutePath() + "' ...");
        if (!DicomFileUtilities.isDicomOrAcrNemaFile(dicomFile)) {
            logInfo("ignored. File: '" + dicomFile.getAbsolutePath() + "' is NOT a DICOM file.", INDENT);
            return;
        }

        AttributeList attributeList = new AttributeList();

        if (_settings.verbose()) {
            logInfo("reading DICOM file: '" + dicomFile.getName() + "' ...", INDENT);
        }
        attributeList.read(dicomFile);

        if (_settings.verbose()) {
            logInfo("editting DICOM object in memory ...", INDENT);
        }
        /*
         * AccessionNumber:
         */
        String accessionNumber = dicomFile.getParentFile().getName();
        String accessionNumberInDicomFile = Attribute.getSingleStringValueOrNull(attributeList,
                TagFromName.AccessionNumber);
        if (!accessionNumber.equals(accessionNumberInDicomFile)) {
            if (_settings.verbose()) {
                logWarning("AccessionNumber: " + accessionNumberInDicomFile + " in file: '"
                        + dicomFile.getAbsolutePath() + "' does not match its directory name. Set AccessionNumber to: "
                        + accessionNumber + ".", INDENT);
            }
            DicomModify.putAttribute(attributeList, TagFromName.AccessionNumber, accessionNumber);
        }
        logInfo("AccessionNumber: " + accessionNumber, INDENT);

        /*
         * SeriesInstanceUID: unique identifier for the series
         */
        String seriesInstanceUID = Attribute.getSingleStringValueOrNull(attributeList, TagFromName.SeriesInstanceUID);
        if (seriesInstanceUID == null) {
            throw new Exception("No SeriesInstanceUID is found in DICOM file header.");
        }

        /*
         * SeriesNumber: series number
         *
         * set SeriesNumber to 1 if it is null, because Mediaflux DICOM engine
         * requires it.
         */
        String seriesNumber = Attribute.getSingleStringValueOrNull(attributeList, TagFromName.SeriesNumber);
        if (seriesNumber == null) {
            DicomModify.putAttribute(attributeList, TagFromName.SeriesNumber, "1");
        }

        /*
         * SOPInstanceUID: unique identifier for the instance/image
         */
        String sopInstanceUID = Attribute.getSingleStringValueOrNull(attributeList, TagFromName.SOPInstanceUID);
        if (sopInstanceUID == null) {
            throw new Exception("No SOPInstanceUID is found in DICOM file header.");
        }

        /*
         * check if the dataset already exists
         */
        String datasetCid = findDicomDataset(cxn, _settings.projectId(), sopInstanceUID, accessionNumber, true);
        if (datasetCid != null) {
            if (_settings.checkCSum()) {
                SimpleEntry<String, Boolean> serverMD5Info = null;
                try {
                    serverMD5Info = getPixelDataChecksum(cxn, datasetCid, true);
                } catch (Throwable e) {
                }
                String serverMD5 = serverMD5Info == null ? null : serverMD5Info.getKey();
                boolean bigEndian = serverMD5Info == null ? false : serverMD5Info.getValue();
                String localMD5 = null;
                try {
                    localMD5 = DicomChecksumUtils.getPixelDataChecksum(attributeList, bigEndian, "md5");
                } catch (Throwable e) {
                }

                if (serverMD5 == null) {
                    if (_settings.verbose()) {
                        logWarning("No MD5 checksum was generated for dataset " + datasetCid, INDENT);
                    }
                } else {
                    if (_settings.verbose()) {
                        logInfo("MD5 checksum: " + serverMD5 + " for dataset " + datasetCid, INDENT);
                    }
                }

                if (localMD5 == null) {
                    if (_settings.verbose()) {
                        logWarning("No MD5 checksum was generated for file: '" + dicomFile.getAbsolutePath() + "'",
                                INDENT);
                    }
                } else {
                    if (_settings.verbose()) {
                        logInfo("MD5 checksum: " + localMD5 + " for file: '" + dicomFile.getAbsolutePath() + "'",
                                INDENT);
                    }
                }

                if (serverMD5 != null && localMD5 != null && serverMD5.equalsIgnoreCase(localMD5)) {
                    if (_settings.verbose()) {
                        logInfo("MD5 checksum: " + localMD5 + " match for dataset " + datasetCid + " and file: '"
                                + dicomFile.getAbsolutePath() + "'", INDENT);
                    }
                } else if (serverMD5 == null && localMD5 == null) {
                    if (_settings.verbose()) {
                        logWarning("No MD5 checksum can be generated on both server dataset " + datasetCid
                                + " and local file: " + dicomFile.getAbsolutePath()
                                + ". The DICOM file may not contain PixelData.", INDENT);
                    }
                } else {
                    throw new Exception("MD5 checksum of file: '" + dicomFile.getAbsolutePath() + "' (" + localMD5
                            + ") of PixelData does not match with dataset " + datasetCid + " (" + serverMD5 + ").");
                }
            }
            logInfo("ignored. File was previous uploaded as dataset " + datasetCid, INDENT);
            return;
        }

        /*
         * modify dicom file
         */
        if (_settings.verbose()) {
            logInfo("modifying temporary copy of file: '" + dicomFile.getAbsolutePath() + "'", INDENT);
        }
        String prefix = dicomFile.getName();
        if (prefix.endsWith(".dcm") || prefix.endsWith(".DCM")) {
            prefix = prefix.substring(0, prefix.length() - 4);
        }
        // File modifiedDicomFile = File.createTempFile(prefix, ".dcm");

        DicomModify.putAttribute(attributeList, TagFromName.PatientName, _settings.projectId());

        String patientId = patientIdMapping.get(accessionNumber);
        if (patientId == null) {
            throw new Exception("Could not find PatientID in mapping file for AccessionNumber: " + accessionNumber);
        }
        DicomModify.putAttribute(attributeList, TagFromName.PatientID, patientId);

        /*
         * find first dataset in the study
         */
        XmlDoc.Element firstDatasetAE = getFirstDicomDataset(cxn, _settings.projectId(), seriesInstanceUID);
        if (firstDatasetAE == null) {
            /*
             * dicom ingest
             */
            logInfo("ingesting dataset...", INDENT);
            String studyCid = DicomIngest.ingest(cxn, attributeList, dicomFile.getAbsolutePath(),
                    _settings.projectId());
            firstDatasetAE = cxn.execute("asset.query", "<action>get-meta</action><size>1</size><where>cid in '"
                    + studyCid + "' and mf-note hasno value</where>").element("asset");

            if (firstDatasetAE == null) {
                throw new Exception("Failed to find the newly ingested DICOM dataset in study " + studyCid
                        + ". (source \"file:" + dicomFile.getAbsolutePath() + "\")");
            }

            String firstDatasetCid = firstDatasetAE.value("cid");

            if (firstDatasetAE.elementExists("lock")) {
                throw new Exception("The newly ingested dataset " + firstDatasetAE.value("cid") + " is locked.");
            }

            logInfo("ingested dataset: " + firstDatasetCid, INDENT);

            // update study name & description
            logInfo("updating metadata for study " + studyCid, INDENT);
            updateStudyName(cxn, studyCid, attributeList);

            // update newly ingested dataset
            logInfo("updating metadata for dataset " + firstDatasetCid, INDENT);
            updateDicomDataset(cxn, firstDatasetAE, dicomFile.getAbsolutePath(), attributeList, _settings.checkCSum());

        } else {
            /*
             * create dataset
             */
            String studyCid = CiteableIdUtils.parent(firstDatasetAE.value("cid"));
            logInfo("creating dataset (in study " + studyCid + ")...", INDENT);
            datasetCid = createDicomDataset(cxn, firstDatasetAE, attributeList, dicomFile.getAbsolutePath(),
                    _settings.checkCSum());
            logInfo("created dataset " + datasetCid, INDENT);
        }
    }

    static Map<String, String> loadPatientIdMapping(File file) throws Throwable {
        Map<String, String> map = new HashMap<String, String>((120000 * 4 + 2) / 3);
        try (Stream<String> stream = Files.lines(file.toPath())) {
            stream.forEach(line -> {
                if (line.matches("^\\ *\\d+\\ *,.+")) {
                    String[] tokens = line.trim().split("\\ *,\\ *");
                    String patientId = tokens[0];
                    String accessionNumber = tokens[1];
                    map.put(accessionNumber, patientId);
                }
            });
        }
        if (map.isEmpty()) {
            throw new IllegalArgumentException("Failed to parse patient id mapping file: " + file.getPath() + ".");
        }
        return map;
    }

    private static String findDicomDataset(ServerClient.Connection cxn, String projectCid, String sopInstanceUID,
            String accessionNumber, boolean exceptionIfMultipleFound) throws Throwable {

        StringBuilder sb = new StringBuilder();
        sb.append("cid starts with '").append(projectCid).append("'");
        sb.append(" and xpath(daris:dicom-dataset/object/de[@tag='00080018']/value)='").append(sopInstanceUID)
                .append("'");
        sb.append(" and xpath(daris:dicom-dataset/object/de[@tag='00080050']/value)='").append(accessionNumber)
                .append("'");

        XmlStringWriter w = new XmlStringWriter();
        w.add("where", sb.toString());
        w.add("action", "get-cid");

        XmlDoc.Element re = cxn.execute("asset.query", w.document());
        int nbResults = re.count("cid");
        if (nbResults > 1 && exceptionIfMultipleFound) {
            StringBuilder sb1 = new StringBuilder();
            Collection<String> cids = re.values("cid");
            for (String cid : cids) {
                sb.append(cid).append(" ");
            }
            throw new Exception("More than one dicom datasets: " + sb1.toString() + "are found with SOPInstanceUID="
                    + sopInstanceUID + " and AccessionNumber=" + accessionNumber + ". Expects only one. ");
        }
        return re.value("cid");
    }

    private static XmlDoc.Element getFirstDicomDataset(ServerClient.Connection cxn, String projectCid,
            String seriesInstanceUID) throws Throwable {

        StringBuilder sb = new StringBuilder();
        sb.append("cid starts with '").append(projectCid).append("'");
        sb.append(" and xpath(mf-dicom-series/uid)='").append(seriesInstanceUID).append("'");
        sb.append(" and mf-note has value");

        XmlStringWriter w = new XmlStringWriter();
        w.add("where", sb.toString());
        w.add("size", 1);
        w.add("action", "get-meta");
        return cxn.execute("asset.query", w.document()).element("asset");
    }

    private static void updateDicomDataset(ServerClient.Connection cxn, XmlDoc.Element ae, String sourcePath,
            AttributeList attributeList, boolean csumCheck) throws Throwable {
        String name = datasetNameFor(attributeList);
        String description = datasetDescriptionFor(attributeList);

        XmlStringWriter w = new XmlStringWriter();
        w.add("id", ae.value("@id"));
        w.push("meta");

        if (name != null || description != null) {
            w.push("daris:pssd-object");
            if (name != null) {
                w.add("name", name);
            }
            if (description != null) {
                w.add("description", description);
            }
            w.pop();
        }

        w.push("daris:pssd-derivation");
        w.add("processed", true);
        w.pop();

        /*
         * mf-note
         */
        w.push("mf-note");
        w.add("note", "source: " + sourcePath);
        w.pop();

        w.pop();

        cxn.execute("asset.set", w.document());

        /*
         * daris:dicom-dataset
         */
        String cid = ae.value("cid");
        populateAdditionalDicomMetadata(cxn, cid);

        if (csumCheck) {
            generatePixelDataChecksum(cxn, cid);
        }
    }

    private static XmlDoc.Element generatePixelDataChecksum(Connection cxn, String datasetCid) throws Throwable {
        XmlStringWriter w = new XmlStringWriter();
        w.add("cid", datasetCid);
        w.add("type", "md5");
        w.add("save", true);
        return cxn.execute("daris.dicom.pixel-data.checksum.generate", w.document());
    }

    private static SimpleEntry<String, Boolean> getPixelDataChecksum(Connection cxn, String datasetCid,
            boolean generate) throws Throwable {
        XmlDoc.Element ae = cxn.execute("asset.get", "<cid>" + datasetCid + "</cid>", null, null).element("asset");
        boolean bigEndian = ae.booleanValue("meta/daris:dicom-pixel-data-checksum/object/@big-endian", false);
        String serverMD5 = ae.value("meta/daris:dicom-pixel-data-checksum/object/pixel-data/csum[@type='md5']");
        if (serverMD5 == null) {
            if (generate) {
                XmlDoc.Element re = generatePixelDataChecksum(cxn, datasetCid);
                bigEndian = re.booleanValue("object/@big-endian", false);
                serverMD5 = re.value("object/pixel-data/csum[@type='md5']");
            } else {
                return null;
            }
        }
        return new SimpleEntry<String, Boolean>(serverMD5, bigEndian);
    }

    static String createDicomDataset(ServerClient.Connection cxn, XmlDoc.Element firstSiblingAE,
            AttributeList attributeList, String sourcePath, boolean csumCheck) throws Throwable {

        String firstDatasetCid = firstSiblingAE.value("cid");
        String studyCid = CiteableIdUtils.parent(firstDatasetCid);
        String exMethodCid = firstSiblingAE.value("meta/daris:pssd-derivation/method");
        String exMethodStep = firstSiblingAE.value("meta/daris:pssd-derivation/method/@step");
        String name = datasetNameFor(attributeList);
        String description = datasetDescriptionFor(attributeList);

        XmlStringWriter w = new XmlStringWriter();
        w.add("pid", studyCid);
        w.push("method");
        w.add("id", exMethodCid);
        w.add("step", exMethodStep);
        w.pop();
        w.add("type", "dicom/series");
        w.add("ctype", "application/arc-archive");
        w.add("processed", true);
        if (name != null) {
            w.add("name", name);
        }
        if (description != null) {
            w.add("description", description);
        }
        // w.add("fillin", false);
        w.push("meta");

        /*
         * mf-dicom-series
         */
        w.push("mf-dicom-series", new String[] { "tag", "pssd.meta", "ns", "dicom" });

        String seriesInstanceUID = Attribute.getSingleStringValueOrNull(attributeList, TagFromName.SeriesInstanceUID);
        w.add("uid", seriesInstanceUID);

        String seriesNumber = Attribute.getSingleStringValueOrNull(attributeList, TagFromName.SeriesNumber);
        if (seriesNumber != null) {
            w.add("id", seriesNumber);
        }

        String seriesDescription = Attribute.getSingleStringValueOrNull(attributeList, TagFromName.SeriesDescription);
        if (seriesDescription != null || seriesNumber != null) {
            w.add("description", seriesDescription == null ? seriesNumber : seriesDescription);
        }

        String seriesDate = Attribute.getSingleStringValueOrNull(attributeList, TagFromName.SeriesDate);
        String seriesTime = Attribute.getSingleStringValueOrNull(attributeList, TagFromName.SeriesTime);
        if (seriesDate != null && seriesTime != null) {
            Date sdate = parseDate(seriesDate + seriesTime);
            w.add("sdate", sdate);
        }

        String acquisitionDate = Attribute.getSingleStringValueOrNull(attributeList, TagFromName.AcquisitionDate);
        String acquisitionTime = Attribute.getSingleStringValueOrNull(attributeList, TagFromName.AcquisitionTime);
        if (acquisitionDate != null && acquisitionTime != null) {
            Date adate = parseDate(acquisitionDate + acquisitionTime);
            w.add("adate", adate);
        }

        String instanceNumber = Attribute.getSingleStringValueOrNull(attributeList, TagFromName.InstanceNumber);
        w.add("imin", instanceNumber);
        w.add("imax", instanceNumber);
        w.add("size", 1);

        String modality = Attribute.getSingleStringValueOrNull(attributeList, TagFromName.Modality);
        w.add("modality", modality);

        String protocolName = Attribute.getSingleStringValueOrNull(attributeList, TagFromName.ProtocolName);
        if (protocolName != null) {
            w.add("protocol", protocolName);
        }

        double[] imagePosition = Attribute.getDoubleValues(attributeList, TagFromName.ImagePositionPatient);
        double[] imageOrientation = Attribute.getDoubleValues(attributeList, TagFromName.ImageOrientationPatient);

        if ((imagePosition != null && imagePosition.length == 3)
                || (imageOrientation != null && imageOrientation.length == 6)) {
            w.push("image");
            if (imagePosition != null) {
                w.push("position");
                w.add("x", imagePosition[0]);
                w.add("y", imagePosition[1]);
                w.add("z", imagePosition[2]);
                w.pop();
            }
            if (imageOrientation != null) {
                w.push("orientation");
                for (int i = 0; i < 6; i++) {
                    w.add("value", imageOrientation[i]);
                }
                w.pop();
            }
            w.pop();
        }

        w.pop();

        /*
         * mf-note
         */
        w.push("mf-note");
        w.add("note", "source: " + sourcePath);
        w.pop();

        w.pop();

        String[] datasetCid = new String[1];
        Archive.declareSupportForAllTypes();
        ServerClient.Input sci = new ServerClient.GeneratedInput("application/arc-archive", "aar", sourcePath, -1,
                null) {

            @Override
            protected void copyTo(OutputStream os, AbortCheck ac) throws Throwable {
                ArchiveOutput ao = ArchiveRegistry.createOutput(os, "application/arc-archive",
                        DicomIngest.Settings.DEFAULT_COMPRESSION_LEVEL, null);

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
        datasetCid[0] = cxn.execute("om.pssd.dataset.derivation.create", w.document(), sci).value("id");

        /*
         * daris:dicom-dataset
         */
        populateAdditionalDicomMetadata(cxn, datasetCid[0]);

        if (csumCheck) {
            generatePixelDataChecksum(cxn, datasetCid[0]);
        }

        return datasetCid[0];

    }

    private static String datasetNameFor(AttributeList attributeList) {
        String protocolName = Attribute.getSingleStringValueOrNull(attributeList, TagFromName.ProtocolName);
        String seriesDescription = Attribute.getSingleStringValueOrNull(attributeList, TagFromName.SeriesDescription);
        StringBuilder sb = new StringBuilder();
        if (seriesDescription != null) {
            if (protocolName != null) {
                if (seriesDescription.startsWith(protocolName)) {
                    sb.append(seriesDescription.replace(',', ' ').replaceAll("\\ {2,}+", "\\ "));
                } else {
                    sb.append(protocolName).append("_").append(seriesDescription);
                }
            }
        } else {
            if (protocolName != null) {
                sb.append(protocolName);
            }
        }
        String viewPosition = Attribute.getSingleStringValueOrNull(attributeList, TagFromName.ViewPosition);
        if (viewPosition != null) {
            if (sb.length() > 0) {
                sb.append("_");
            }
            sb.append(viewPosition);
        }
        String imageLaterality = Attribute.getSingleStringValueOrNull(attributeList, TagFromName.ImageLaterality);
        if (imageLaterality != null) {
            if (sb.length() > 0) {
                sb.append("_");
            }
            sb.append(imageLaterality);
        }
        if (sb.length() > 0) {
            return sb.toString();
        } else {
            return null;
        }
    }

    private static String datasetDescriptionFor(AttributeList attributeList) {
        String seriesDescription = Attribute.getSingleStringValueOrNull(attributeList, TagFromName.SeriesDescription);
        StringBuilder sb = new StringBuilder();
        if (seriesDescription != null) {
            sb.append(seriesDescription.replace(',', ' ').replaceAll("\\ {2,}+", "\\ ")).append(", ");
        }
        String imageType = Attribute.getDelimitedStringValuesOrNull(attributeList, TagFromName.ImageType);
        if (imageType != null) {
            sb.append(imageType).append(", ");
        }
        String viewPosition = Attribute.getSingleStringValueOrNull(attributeList, TagFromName.ViewPosition);
        if (viewPosition != null) {
            sb.append(viewPosition).append(", ");
        }
        String imageLaterality = Attribute.getSingleStringValueOrNull(attributeList, TagFromName.ImageLaterality);
        if (imageLaterality != null) {
            sb.append(imageLaterality).append(", ");
        }
        if (sb.length() > 0) {
            return sb.toString();
        } else {
            return null;
        }
    }

    private static String studyNameFor(AttributeList attributeList) {
        String studyDescription = Attribute.getSingleStringValueOrNull(attributeList, TagFromName.StudyDescription);
        String accessionNumber = Attribute.getSingleStringValueOrNull(attributeList, TagFromName.AccessionNumber);
        StringBuilder sb = new StringBuilder();
        if (studyDescription != null) {
            sb.append(studyDescription).append(" - ");
        }
        sb.append(accessionNumber);
        return sb.toString();
    }

    private static Date parseDate(String dateTime) throws Throwable {
        int idx = dateTime.indexOf('.');
        if (idx == -1) {
            return new SimpleDateFormat("yyyyMMddHHmmss").parse(dateTime);
        } else {
            Date date = new SimpleDateFormat("yyyyMMddHHmmss").parse(dateTime.substring(0, idx));
            int millisecs = (int) (Double.parseDouble("0" + dateTime.substring(idx)) * 1000.0);
            return new Date(date.getTime() + millisecs);
        }
    }

    private static void populateAdditionalDicomMetadata(ServerClient.Connection cxn, String datasetCid)
            throws Throwable {

        XmlStringWriter w = new XmlStringWriter();
        w.add("cid", datasetCid);

        // NOTE: code commented out below are replaced by calling
        // vicnode.daris.lifepool.metadata.extract service
        // @formatter:off      
//        w.add("doc-tag", "pssd.meta");
//        w.add("if-exists", "merge");
//        w.add("tag", "00080008"); // ImageType
//        w.add("tag", "00080018"); // SOPInstanceUID
//        w.add("tag", "00080050"); // AccessionNumber
//        w.add("tag", "00080060"); // Modality
//        w.add("tag", "00080068"); // PresentationIntentType
//        w.add("tag", "00080070"); // Manufacturer
//        w.add("tag", "00080080"); // InstitutionName
//        w.add("tag", "0008103E"); // SeriesDescription
//        w.add("tag", "00081090"); // ManufacturerModelName
//        w.add("tag", "00181400"); // AcquisitionDeviceProcessingDescription
//        w.add("tag", "00185101"); // ViewPosition
//        w.add("tag", "00200062"); // ImageLaterality
//        cxn.execute("dicom.metadata.populate", w.document());
        // @formatter:on

        cxn.execute("vicnode.daris.lifepool.metadata.extract", w.document());
    }

    private static void updateStudyName(ServerClient.Connection cxn, String studyCid, AttributeList attributeList)
            throws Throwable {

        XmlDoc.Element ae = cxn.execute("asset.get", "<cid>" + studyCid + "</cid>");
        String studyName = studyNameFor(attributeList);
        if (!studyName.equals(ae.value("meta/daris:pssd-object/name"))) {
            XmlStringWriter w = new XmlStringWriter();
            w.add("cid", studyCid);
            w.push("meta");
            w.push("daris:pssd-object", new String[] { "id", ae.value("meta/daris:pssd-object/@id") });
            w.add("name", studyName);
            w.add("description", studyName);
            w.pop();
            w.pop();
            cxn.execute("asset.set", w.document());
        }

    }

}
