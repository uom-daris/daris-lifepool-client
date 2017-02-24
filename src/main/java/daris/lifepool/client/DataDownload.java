package daris.lifepool.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import arc.archive.ArchiveInput;
import arc.archive.ArchiveRegistry;
import arc.mf.client.RemoteServer;
import arc.mf.client.ServerClient;
import arc.mf.client.archive.Archive;
import arc.mime.NamedMimeType;
import arc.streams.LongInputStream;
import arc.streams.StreamCopy;
import arc.xml.XmlDoc.Element;
import arc.xml.XmlStringWriter;
import daris.lifepool.client.query.Query;
import daris.lifepool.client.query.QueryManifestParser;

public class DataDownload {

    public static String LAYOUT_PATTERN = "xpath(daris:dicom-dataset/object/de[@tag='00080050']/value)";

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
        File manifestFile = null;
        Boolean includeNull = null;
        Boolean extract = null;
        File outputDir = null;
        File outputZipFile = null;
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
                } else if (args[i].equals("--manifest")) {
                    if (manifestFile != null) {
                        throw new Exception("--manifest has already been specified.");
                    }
                    manifestFile = new File(args[i + 1]);
                    if (!manifestFile.exists()) {
                        throw new FileNotFoundException("File " + args[i + 1] + " is not found.");
                    }
                    i += 2;
                } else if (args[i].equals("--include-null")) {
                    if (includeNull != null) {
                        throw new Exception("--include-null has been specified more than once.");
                    }
                    includeNull = true;
                    i++;
                } else if (args[i].equals("--extract")) {
                    if (extract != null) {
                        throw new Exception("--extract has been specified more than once.");
                    }
                    extract = true;
                    i++;
                } else {
                    if (outputDir != null) {
                        throw new Exception("Output directory has been specified more than once.");
                    }
                    outputDir = new File(args[i]);
                    i++;
                }
            }
            if (pid == null) {
                throw new Exception("--pid is not specified.");
            }
            if (manifestFile == null) {
                throw new Exception("--manifest is not specified.");
            }
            if (outputDir == null) {
                outputDir = Paths.get(System.getProperty("user.dir")).toFile();
            }
            if (includeNull == null) {
                includeNull = false;
            }
            if (extract == null) {
                extract = false;
            }
            if (!extract) {
                outputZipFile = new File(outputDir, outputZipFileNameFor(manifestFile.getName()));
                if (outputZipFile.exists()) {
                    throw new Exception("Output file: \"" + outputZipFile.getCanonicalPath() + "\" already exists.");
                }
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

            System.out.print("parsing manifest file: " + manifestFile.getCanonicalPath() + "...");
            List<Query> queries = QueryManifestParser.parse(manifestFile, !includeNull);
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
                System.out.print("executing queries and downloading results...");
                download(cxn, pid, queries, extract ? outputDir : outputZipFile);
                System.out.println("done.");
            } finally {
                cxn.closeAndDiscard();
            }
        } catch (IllegalArgumentException ex) {
            System.err.println("Error: " + ex.getMessage());
            showHelp();
        }
    }

    public static void download(ServerClient.Connection cxn, String pid, List<Query> queries, File out)
            throws Throwable {
        XmlStringWriter w = new XmlStringWriter();
        w.add("where", Query.execute(cxn, pid, queries));
        w.add("cid", pid);
        w.add("format", out.isDirectory() ? "aar" : "zip");
        w.add("parts", "content");
        w.add("include-attachments", false);
        w.add("decompress", true);
        w.add("layout-pattern", new String[] { "type", "dataset" }, DataDownload.LAYOUT_PATTERN);
        if (out.isDirectory()) {
            if (!Files.exists(out.toPath())) {
                Files.createDirectories(out.toPath());
            }
            cxn.execute("daris.collection.archive.create", w.document(), null, new ServerClient.OutputConsumer() {

                @Override
                protected void consume(Element re, LongInputStream in) throws Throwable {
                    Archive.declareSupportForAllTypes();
                    ArchiveInput ai = ArchiveRegistry.createInput(in, new NamedMimeType("application/arc-archive"));
                    ArchiveInput.Entry e;
                    try {
                        while ((e = ai.next()) != null) {
                            try {
                                if (e.isDirectory()) {
                                    new File(out, e.name()).mkdirs();
                                } else {
                                    StreamCopy.copy(e.stream(), new File(out, e.name()));
                                }
                            } finally {
                                ai.closeEntry();
                            }
                        }
                    } finally {
                        try {
                            ai.close();
                        } finally {
                            in.close();
                        }
                    }
                }
            });
        } else {
            cxn.execute("daris.collection.archive.create", w.document(), null, new ServerClient.FileOutput(out));
        }

    }

    private static String outputZipFileNameFor(String manifestFileName) {
        StringBuilder sb = new StringBuilder();

        // remove file ext
        int idx = manifestFileName.lastIndexOf('.');
        if (idx > 0) {
            sb.append(manifestFileName.substring(0, idx));
        } else {
            sb.append(manifestFileName);
        }

        sb.append('-');

        sb.append("result-");

        // append time stamp
        sb.append(new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date()));

        // append zip suffix
        sb.append(".zip");
        return sb.toString();
    }

    private static void showHelp() {
        System.out.println(
                "Usage: data-download [--help] --mf.host <host> --mf.port <port> --mf.transport <transport> [--mf.sid <sid>|--mf.token <token>|--mf.auth <domain,user,password>] --pid <project-cid> --manifest <manifest-file> [output-directory]");
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
                "    --manifest <query-manifest-file>     The query manifest file in CSV format. Specification of the format see https://docs.google.com/document/d/1skiNkR8lxx_cW9pCW2OEsZn30wYrkLr8n4LR1OgUmMU");
        System.out.println(
                "    --include-null                       If specified, include the asset/image, if a DICOM element is not specified in the manifest, and the element of the asset/image has no value.");
        System.out.println(
                "    --extract                            Extract the downloaded archive. If not specified, the downloaded data will be save to a zip archive.");
        System.out.println("    --help                               Display help information.");
    }

}
