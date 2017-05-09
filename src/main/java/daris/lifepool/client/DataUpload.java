package daris.lifepool.client;

import java.io.File;
import java.io.FileNotFoundException;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.pixelmed.dicom.Attribute;
import com.pixelmed.dicom.AttributeList;
import com.pixelmed.dicom.DicomFileUtilities;
import com.pixelmed.dicom.TagFromName;
import com.pixelmed.dicom.TransferSyntax;

import arc.archive.ArchiveOutput;
import arc.archive.ArchiveRegistry;
import arc.mf.client.RemoteServer;
import arc.mf.client.ServerClient;
import arc.mf.client.archive.Archive;
import arc.streams.StreamCopy.AbortCheck;
import arc.xml.XmlDoc;
import arc.xml.XmlStringWriter;
import daris.lifepool.client.dicom.DicomIngest;
import daris.lifepool.client.dicom.DicomModify;

public class DataUpload {

    public static final String DEFAULT_AE_TITLE = "DARIS_LIFEPOOL_CLIENT";

    public static void main(String[] args) throws Throwable {
        if (args == null || args.length == 0) {
            showHelp();
            System.exit(1);
        }
        String mfHost = null;
        int mfPort = -1;
        String mfTransport = null;
        boolean useHttp = true;
        boolean encrypt = true;
        String mfAuth = null;
        String mfToken = null;
        String mfSid = null;
        String pid = null;
        File patientIdMapFile = null;
        List<File> inputs = new ArrayList<File>();
        Boolean continueOnError = null;
        try {
            for (int i = 0; i < args.length;) {
                if (args[i].equals("--help") || args[i].equals("-h")) {
                    showHelp();
                    System.exit(0);
                } else if (args[i].equals("--mf.host")) {
                    if (mfHost != null) {
                        throw new Exception("--mf.host has already been specified.");
                    }
                    mfHost = args[i + 1];
                    i += 2;
                } else if (args[i].equals("--mf.port")) {
                    if (mfPort > 0) {
                        throw new Exception("--mf.port has already been specified.");
                    }
                    try {
                        mfPort = Integer.parseInt(args[i + 1]);
                    } catch (Throwable e) {
                        throw new Exception("Invalid mf.port: " + args[i + 1], e);
                    }
                    if (mfPort <= 0 || mfPort > 65535) {
                        throw new Exception("Invalid mf.port: " + args[i + 1]);
                    }
                    i += 2;
                } else if (args[i].equals("--mf.transport")) {
                    if (mfTransport != null) {
                        throw new Exception("--mf.transport has already been specified.");
                    }
                    mfTransport = args[i + 1];
                    i += 2;
                    if ("http".equalsIgnoreCase(mfTransport)) {
                        useHttp = true;
                        encrypt = false;
                    } else if ("https".equalsIgnoreCase(mfTransport)) {
                        useHttp = true;
                        encrypt = true;
                    } else if ("tcp/ip".equalsIgnoreCase(mfTransport)) {
                        useHttp = false;
                        encrypt = false;
                    } else {
                        throw new Exception(
                                "Invalid mf.transport: " + mfTransport + ". Expects http, https or tcp/ip.");
                    }
                } else if (args[i].equals("--mf.auth")) {
                    if (mfAuth != null) {
                        throw new Exception("--mf.auth has already been specified.");
                    }
                    if (mfSid != null || mfToken != null) {
                        throw new Exception(
                                "You can only specify one of mf.auth, mf.token or mf.sid. Found more than one.");
                    }
                    mfAuth = args[i + 1];
                    i += 2;
                } else if (args[i].equals("--mf.token")) {
                    if (mfToken != null) {
                        throw new Exception("--mf.token has already been specified.");
                    }
                    if (mfSid != null || mfAuth != null) {
                        throw new Exception(
                                "You can only specify one of mf.auth, mf.token or mf.sid. Found more than one.");
                    }
                    mfToken = args[i + 1];
                    i += 2;
                } else if (args[i].equals("--mf.sid")) {
                    if (mfSid != null) {
                        throw new Exception("--mf.sid has already been specified.");
                    }
                    if (mfToken != null || mfAuth != null) {
                        throw new Exception(
                                "You can only specify one of mf.auth, mf.token or mf.sid. Found more than one.");
                    }
                    mfSid = args[i + 1];
                    i += 2;
                } else if (args[i].equals("--pid")) {
                    if (pid != null) {
                        throw new Exception("--pid has already been specified.");
                    }
                    pid = args[i + 1];
                    i += 2;
                } else if (args[i].equals("--patient.id.map")) {
                    if (patientIdMapFile != null) {
                        throw new Exception("--patient.id.map has already been specified.");
                    }
                    patientIdMapFile = new File(args[i + 1]);
                    if (!patientIdMapFile.exists()) {
                        throw new FileNotFoundException("File " + args[i + 1] + " is not found.");
                    }
                    i += 2;
                } else if (args[i].equals("--continue-on-error")) {
                    if (continueOnError != null) {
                        throw new Exception("--continue-on-error has already been specified.");
                    }
                    continueOnError = true;
                    i++;
                } else {
                    File input = new File(args[i]);
                    if (!input.exists()) {
                        throw new FileNotFoundException("File " + args[i] + " is not found.");
                    }
                    inputs.add(input);
                    i++;
                }
            }
            if (pid == null) {
                throw new Exception("--pid is not specified.");
            }
            if (patientIdMapFile == null) {
                throw new Exception("--patient.id.map is not specified.");
            }
            if (inputs.isEmpty()) {
                throw new Exception("No input dicom file/directory is specified.");
            }
            if (mfHost == null) {
                throw new Exception("--mf.host is not specified.");
            }
            if (mfPort <= 0) {
                throw new Exception("--mf.port is not specified.");
            }
            if (mfTransport == null) {
                throw new Exception("--mf.transport is not specified.");
            }
            if (mfAuth == null && mfSid == null && mfToken == null) {
                throw new Exception("You need to specify one of mf.auth, mf.token or mf.sid. Found none.");
            }
            if (continueOnError == null) {
                continueOnError = false;
            }
            final boolean stopOnError = !continueOnError;
            System.out.print("loading accession.number->patient.id mapping file: " + patientIdMapFile.getCanonicalPath()
                    + "...");
            Map<String, String> patientIdMap = loadPatientIdMap(patientIdMapFile);
            System.out.println("done.");
            RemoteServer server = new RemoteServer(mfHost, mfPort, useHttp, encrypt);
            final ServerClient.Connection cxn = server.open();
            try {
                if (mfToken != null) {
                    cxn.connectWithToken(mfToken);
                } else if (mfAuth != null) {
                    String[] parts = mfAuth.split(",");
                    if (parts.length != 3) {
                        throw new Exception("Invalid mf.auth: " + mfAuth
                                + ". Expects a string in the form of 'domain,user,password'");
                    }
                    cxn.connect(parts[0], parts[1], parts[2]);
                } else {
                    cxn.reconnect(mfSid);
                }
                final String projectCid = pid;
                for (File input : inputs) {
                    if (Files.isDirectory(input.toPath())) {
                        Files.walkFileTree(input.toPath(), new SimpleFileVisitor<Path>() {
                            @Override
                            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                                try {
                                    uploadDicomFile(cxn, path.toFile(), projectCid, patientIdMap);
                                } catch (Throwable e) {
                                    e.printStackTrace(System.err);
                                    if (stopOnError) {
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
                                if (stopOnError) {
                                    return FileVisitResult.TERMINATE;
                                } else {
                                    return FileVisitResult.CONTINUE;
                                }
                            }
                        });
                    } else {
                        try {
                            uploadDicomFile(cxn, input, projectCid, patientIdMap);
                        } catch (Throwable e) {
                            if (stopOnError) {
                                throw e;
                            } else {
                                e.printStackTrace(System.err);
                            }
                        }
                    }
                }
            } finally {
                cxn.closeAndDiscard();
            }
        } catch (IllegalArgumentException ex) {
            System.err.println("Error: " + ex.getMessage());
            showHelp();
        }
    }

    private static void showHelp() {
        System.out.println(
                "Usage: data-upload [--help] --mf.host <host> --mf.port <port> --mf.transport <transport> [--mf.sid <sid>|--mf.token <token>|--mf.auth <domain,user,password>] [--continue-on-error] --pid <project-cid> <dicom-files/dicom-directories>");
        System.out.println("Description:");
        System.out.println("    --mf.host <host>                     The Mediaflux server host.");
        System.out.println("    --mf.port <port>                     The Mediaflux server port.");
        System.out.println(
                "    --mf.transport <transport>           The Mediaflux server transport, can be http, https or tcp/ip.");
        System.out.println("    --mf.auth <domain,user,password>     The Mediaflux user authentication deatils.");
        System.out.println("    --mf.token <token>                   The Mediaflux secure identity token.");
        System.out.println("    --mf.sid <sid>                       The Mediaflux session id.");
        System.out.println("    --pid <project-cid>                  The DaRIS project cid.");
        System.out.println(
                "    --patient.id.map <paitent-id-map>    The file contains AccessionNumber -> PatientID mapping.");
        System.out.println(
                "    --continue-on-error                  Continue to upload remaining input files when error occurs.");
        System.out.println("    --help                               Display help information.");
    }

    public static void uploadDicomFile(ServerClient.Connection cxn, File dicomFile, String projectCid,
            Map<String, String> patientIdMap) throws Throwable {

        if (!DicomFileUtilities.isDicomOrAcrNemaFile(dicomFile)) {
            System.out.println("\"" + dicomFile.getCanonicalPath() + "\" is not a dicom file.");
            return;
        }

        AttributeList attributeList = new AttributeList();

        System.out.println("###DEBUG:### begin reading source dicom file. " + new Date());
        attributeList.read(dicomFile);
        System.out.println("###DEBUG:###   end reading source dicom file. " + new Date());

        System.out.println("###DEBUG:### begin editing in-memory dicom object. " + new Date());
        /*
         * AccessionNumber:
         */
        String accessionNumber = dicomFile.getParentFile().getName();
        String accessionNumberInDicomFile = Attribute.getSingleStringValueOrNull(attributeList,
                TagFromName.AccessionNumber);
        if (!accessionNumber.equals(accessionNumberInDicomFile)) {
            System.out
                    .println("Warning: The AccessionNumber: " + accessionNumberInDicomFile + " in DICOM header of file:"
                            + dicomFile.getCanonicalPath() + " does not match parent directory name: " + accessionNumber
                            + ". Set the value to " + accessionNumber + ".");
            DicomModify.putAttribute(attributeList, TagFromName.AccessionNumber, accessionNumber);
        }
        System.out.println();
        System.out.println("### AccessionNumber(0008,0050): " + accessionNumber);
        System.out.println("### File: " + dicomFile.getCanonicalPath());
        System.out.println();

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
        String datasetCid = findDicomDataset(cxn, projectCid, sopInstanceUID, accessionNumber, true);
        if (datasetCid != null) {
            System.out.println("Dicom dataset " + datasetCid + " created from local file \""
                    + dicomFile.getCanonicalPath() + "\" already exists.");
            return;
        }

        /*
         * modify dicom file
         */
        System.out.println("Modify temporary copy of Dicom file");
        String prefix = dicomFile.getName();
        if (prefix.endsWith(".dcm") || prefix.endsWith(".DCM")) {
            prefix = prefix.substring(0, prefix.length() - 4);
        }
        // File modifiedDicomFile = File.createTempFile(prefix, ".dcm");

        DicomModify.putAttribute(attributeList, TagFromName.PatientName, projectCid);

        String patientId = patientIdMap.get(accessionNumber);
        if (patientId == null) {
            throw new Exception("Could not find PatientID in mapping file for AccessionNumber: " + accessionNumber);
        }
        DicomModify.putAttribute(attributeList, TagFromName.PatientID, patientId);

        System.out.println("###DEBUG:###   end editing in-memory dicom object. " + new Date());
        /*
         * find first dataset in the study
         */
        XmlDoc.Element firstDatasetAE = getFirstDicomDataset(cxn, projectCid, seriesInstanceUID);
        if (firstDatasetAE == null) {
            /*
             * dicom ingest
             */
            System.out.println("###DEBUG:### begin ingesting dicom object. " + new Date());
            System.out.print("ingesting data from file: \"" + dicomFile.getCanonicalPath() + "\"...");
            String studyCid = DicomIngest.ingest(cxn, attributeList, dicomFile.getCanonicalPath(), projectCid);
            firstDatasetAE = cxn.execute("asset.query", "<action>get-meta</action><size>1</size><where>cid in '"
                    + studyCid + "' and mf-note hasno value</where>").element("asset");

            if (firstDatasetAE == null) {
                throw new Exception("Failed to find the newly ingested DICOM dataset in study " + studyCid
                        + ". (source \"file:" + dicomFile.getCanonicalPath() + "\")");
            }

            String firstDatasetCid = firstDatasetAE.value("cid");

            if (firstDatasetAE.elementExists("lock")) {
                throw new Exception("The newly ingested dataset " + firstDatasetAE.value("cid") + " is locked.");
            }
            System.out.println("ingested dataset " + firstDatasetCid + " (in study " + studyCid + ").");
            System.out.println("###DEBUG:###   end ingesting dicom object. " + new Date());

            System.out.println("###DEBUG:### begin updating study & dataset metadata. " + new Date());
            // update study name & description
            System.out.print("updating study " + studyCid + "(accession.number=" + accessionNumber + ")...");
            updateStudyName(cxn, studyCid, attributeList);
            System.out.println("done.");

            // update newly ingested dataset
            System.out.print("updating dataset " + firstDatasetCid + "...");
            updateDicomDataset(cxn, firstDatasetAE, dicomFile.getCanonicalPath(), attributeList);
            System.out.println("done.");
            System.out.println("###DEBUG:###   end updating study & dataset metadata. " + new Date());
        } else {
            /*
             * create dataset
             */
            System.out.println("###DEBUG:### begin creating dicom dataset. " + new Date());
            String studyCid = CiteableIdUtils.parent(firstDatasetAE.value("cid"));
            System.out.print("creating dataset from file: \"" + dicomFile.getCanonicalPath() + "\" in study " + studyCid
                    + "...");
            datasetCid = createDicomDataset(cxn, firstDatasetAE, attributeList, dicomFile.getCanonicalPath());
            System.out.println("created dataset " + datasetCid + ".");
            System.out.println("###DEBUG:###   end creating dicom dataset. " + new Date());
        }
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
            AttributeList attributeList) throws Throwable {
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
        populateAdditionalDicomMetadata(cxn, ae.value("cid"));
    }

    static String createDicomDataset(ServerClient.Connection cxn, XmlDoc.Element firstSiblingAE,
            AttributeList attributeList, String sourcePath) throws Throwable {

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

    private static Map<String, String> loadPatientIdMap(File file) throws Throwable {
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
            throw new Exception("Failed to parse patient id mapping file: " + file.getCanonicalPath() + ".");
        }
        return map;
    }

}
