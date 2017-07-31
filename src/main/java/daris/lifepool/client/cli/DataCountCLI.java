package daris.lifepool.client.cli;

import daris.lifepool.client.DataCount;
import daris.lifepool.client.DataCount.ProjectSummary;
import daris.lifepool.client.DataCountSettings;
import daris.util.ByteUtils;

public class DataCountCLI {
    public static void main(String[] args) throws Throwable {
        /*
         * load, parse & validate settings
         */
        DataCountSettings settings = new DataCountSettings(null);
        try {
            settings.loadFromDefaultPropertiesFile();
            for (int i = 0; i < args.length;) {
                if (args[i].equals("--help") || args[i].equals("-h")) {
                    showHelp();
                    System.exit(0);
                } else if (args[i].equals("--mf.host")) {
                    settings.setServerHost(args[i + 1]);
                    i += 2;
                } else if (args[i].equals("--mf.port")) {
                    try {
                        settings.setServerPort(Integer.parseInt(args[i + 1]));
                    } catch (Throwable e) {
                        throw new IllegalArgumentException("Invalid mf.port: " + args[i + 1], e);
                    }
                    i += 2;
                } else if (args[i].equals("--mf.transport")) {
                    settings.setServerTransport(args[i + 1]);
                    i += 2;
                } else if (args[i].equals("--mf.auth")) {
                    String auth = args[i + 1];
                    String[] parts = auth.split(",");
                    if (parts == null || parts.length != 3) {
                        throw new IllegalArgumentException("Invalid mf.auth: " + auth);
                    }
                    settings.setUserCredentials(parts[0], parts[1], parts[2]);
                    i += 2;
                } else if (args[i].equals("--mf.token")) {
                    settings.setToken(args[i + 1]);
                    i += 2;
                } else if (args[i].equals("--mf.sid")) {
                    settings.setSessionKey(args[i + 1]);
                    i += 2;
                } else if (args[i].equals("--pid")) {
                    settings.setProjectId(args[i + 1]);
                    i += 2;
                } else {
                    i++;
                }
            }
            settings.validate();
        } catch (IllegalArgumentException ex) {
            System.err.println("Error: " + ex.getMessage());
            showHelp();
            throw ex;
        }

        /*
         * count
         */
        ProjectSummary summary = new DataCount(settings).call();
        if (summary != null) {
            System.out.println(String.format("%20s: %s", "DaRIS Project ID", settings.projectId()));
            System.out.println(String.format("%20s: %d", "Number of Patients", summary.numberofPatients()));
            System.out.println(String.format("%20s: %d", "Number of Accessions", summary.numberOfAccessions()));
            System.out.println(String.format("%20s: %d", "Number of Images", summary.numberOfImages()));
            System.out.println(String.format("%20s: %d Bytes (%s)", "Total Storage Usage", summary.totalStorageUsage(),
                    ByteUtils.getHumanReadableSize(summary.totalStorageUsage())));
        }
    }

    private static void showHelp() {
        // @formatter:off
        System.out.println();
        System.out.println("Usage: daris-lifepool-data-liset [--help] --mf.host <host> --mf.port <port> --mf.transport <transport> [--mf.token <token>|--mf.auth <domain,user,password>|--mf.sid <sid>] --pid <project-cid> [--patient-id <patient-ids>] [accession numbers]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("    --mf.host <host>                     The Mediaflux server host.");
        System.out.println("    --mf.port <port>                     The Mediaflux server port.");
        System.out.println("    --mf.transport <transport>           The Mediaflux server transport, can be http, https or tcp/ip.");
        System.out.println("    --mf.auth <domain,user,password>     The Mediaflux user authentication deatils.");
        System.out.println("    --mf.token <token>                   The Mediaflux secure identity token.");
        System.out.println("    --mf.sid <sid>                       The Mediaflux session id.");
        System.out.println("    --pid <project-cid>                  The DaRIS project cid.");
        System.out.println("    --patient-id <patient-ids>           One or more patient ids, separated with commas to select the images.");
        System.out.println();
        System.out.println("Switches:");        
        System.out.println("    --help                               Display help information.");
        System.out.println();
        System.out.println("Arguments:");        
        System.out.println("    [accession numbers]                  One or more accession numbers to select the images.");        
        System.out.println();
        // @formatter:on
    }
}
