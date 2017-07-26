package daris.lifepool.client.upload;

import java.io.File;

public class UploadCLI {

    public static void main(String[] args) throws Throwable {
        /*
         * load, parse & validate settings
         */
        UploadSettings settings = Upload.loadSettingsFromPropertiesFile();
        try {
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
                } else if (args[i].equals("--patient.id.map")) {
                    File file = new File(args[i + 1]);
                    if (!file.exists()) {
                        throw new IllegalArgumentException("File '" + args[i + 1] + "' is not found.");
                    }
                    settings.setPatientIdMappingFile(file);
                    i += 2;
                } else if (args[i].equals("--continue-on-error")) {
                    settings.setContinueOnError(true);
                    i++;
                } else if (args[i].equals("--csum")) {
                    settings.setCheckCSum(true);
                    i++;
                } else if (args[i].equals("--verbose")) {
                    settings.setVerbose(true);
                    i++;
                } else {
                    File input = new File(args[i]);
                    if (!input.exists()) {
                        throw new IllegalArgumentException("File " + args[i] + " is not found.");
                    }
                    settings.addFile(input);
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
        new Upload(settings).call();
    }

    private static void showHelp() {
        // @formatter:off
        System.out.println();
        System.out.println("Usage: " + Upload.APP + " [--help] --mf.host <host> --mf.port <port> --mf.transport <transport> [--mf.token <token>|--mf.auth <domain,user,password>|--mf.sid <sid>] [--csum] [--continue-on-error] --pid <project-cid> <dicom-files/dicom-directories>");
        System.out.println("Description:");
        System.out.println("    --mf.host <host>                     The Mediaflux server host.");
        System.out.println("    --mf.port <port>                     The Mediaflux server port.");
        System.out.println("    --mf.transport <transport>           The Mediaflux server transport, can be http, https or tcp/ip.");
        System.out.println("    --mf.auth <domain,user,password>     The Mediaflux user authentication deatils.");
        System.out.println("    --mf.token <token>                   The Mediaflux secure identity token.");
        System.out.println("    --mf.sid <sid>                       The Mediaflux session id.");
        System.out.println("    --pid <project-cid>                  The DaRIS project cid.");
        System.out.println("    --patient.id.map <paitent-id-map>    The file contains AccessionNumber -> PatientID mapping.");
        System.out.println("    --csum                               Generate and compare MD5 checksums of PixelData.");
        System.out.println("    --continue-on-error                  Continue to upload remaining input files when error occurs.");
        System.out.println("    --verbose                            Show detailed progress information.");
        System.out.println("    --help                               Display help information.");
        System.out.println();
        // @formatter:on
    }

}
