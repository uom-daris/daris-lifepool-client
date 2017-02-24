package daris.lifepool.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

import arc.mf.client.RemoteServer;
import arc.mf.client.ServerClient;
import arc.xml.XmlStringWriter;
import daris.lifepool.client.query.Query;
import daris.lifepool.client.query.QueryManifestParser;

public class DataShare {

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
                } else {
                    throw new IllegalArgumentException("Unexpected argument: " + args[i]);
                }
            }
            if (pid == null) {
                throw new Exception("--pid is not specified.");
            }
            if (manifestFile == null) {
                throw new Exception("--manifest is not specified.");
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
            if (includeNull == null) {
                includeNull = false;
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
                String url = generateUrl(cxn, pid, queries);
                System.out.println(url);
            } finally {
                cxn.closeAndDiscard();
            }
        } catch (IllegalArgumentException ex) {
            System.err.println("Error: " + ex.getMessage());
            showHelp();
        }
    }

    public static String generateUrl(ServerClient.Connection cxn, String pid, List<Query> queries) throws Throwable {
        XmlStringWriter w = new XmlStringWriter();
        w.add("where", Query.execute(cxn, pid, queries));
        w.add("cid", pid);
        w.add("format", "zip");
        w.add("parts", "content");
        w.add("include-attachments", false);
        w.add("decompress", true);
        w.add("layout-pattern", new String[] { "type", "dataset" }, DataDownload.LAYOUT_PATTERN);

        // TODO
        return null;
    }

    private static void showHelp() {
        System.out.println(
                "Usage: data-share [--help] --mf.host <host> --mf.port <port> --mf.transport <transport> [--mf.sid <sid>|--mf.token <token>|--mf.auth <domain,user,password>] --pid <project-cid> --manifest <manifest-file>");
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
        System.out.println("    --help                               Display help information.");
    }

}
