package daris.lifepool.client.cli;

import java.util.List;

import daris.lifepool.client.DataList;
import daris.lifepool.client.DataList.ResultEntry;
import daris.lifepool.client.DataListSettings;

public class DataListCLI {
    public static void main(String[] args) throws Throwable {
        /*
         * load, parse & validate settings
         */
        DataListSettings settings = new DataListSettings(null);
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
                    settings.addAccessionNumber(args[i]);
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
         * upload
         */
        List<ResultEntry> entries = new DataList(settings).call();
        if (entries != null) {
            System.out.println(String.format("%-32s    %-16s    %s", "DaRIS_ID", "Accession_Number", "File_Name"));
            for (ResultEntry entry : entries) {
                System.out.println(
                        String.format("%-32s    %-16s    %s", entry.cid(), entry.accessionNumber(), entry.fileName()));
            }
        }
    }

    private static void showHelp() {
        // @formatter:off
        System.out.println();
        System.out.println("Usage: daris-lifepool-data-liset [--help] --mf.host <host> --mf.port <port> --mf.transport <transport> [--mf.token <token>|--mf.auth <domain,user,password>|--mf.sid <sid>] --pid <project-cid> <accession numbers>");
        System.out.println();
        System.out.println("Options:");
        System.out.println("    --mf.host <host>                     The Mediaflux server host.");
        System.out.println("    --mf.port <port>                     The Mediaflux server port.");
        System.out.println("    --mf.transport <transport>           The Mediaflux server transport, can be http, https or tcp/ip.");
        System.out.println("    --mf.auth <domain,user,password>     The Mediaflux user authentication deatils.");
        System.out.println("    --mf.token <token>                   The Mediaflux secure identity token.");
        System.out.println("    --mf.sid <sid>                       The Mediaflux session id.");
        System.out.println("    --pid <project-cid>                  The DaRIS project cid.");
        System.out.println();
        System.out.println("Switches:");        
        System.out.println("    --help                               Display help information.");
        System.out.println();
        // @formatter:on
    }
}
