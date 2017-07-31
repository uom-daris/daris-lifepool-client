# daris-lifepool-client

## I. Installation

  * *Pre-requsites*
    * Java 8
  * Download from https://github.com/uom-daris/daris-lifepool-client/releases/latest
  * Extract daris-lifepool-client-x.y.z.zip
  

## II. Command Line Untilities

### 1.1. daris-lifepool-data-upload

The utility to upload local dicom files to DaRIS repository. The local data must conform the following directory struture:
     <Accession>/<SOPInstnaceUID>.dcm

  * *Usage:*
```
Usage: daris-lifepool-data-upload [--help] --mf.host <host> --mf.port <port> --mf.transport <transport> [--mf.token <token>|--mf.auth <domain,user,password>|--mf.sid <sid>] [--csum] [--continue-on-error] [--verbose] --pid <project-cid> <dicom-files/dicom-directories>

Options:
    --mf.host <host>                     The Mediaflux server host.
    --mf.port <port>                     The Mediaflux server port.
    --mf.transport <transport>           The Mediaflux server transport, can be http, https or tcp/ip.
    --mf.auth <domain,user,password>     The Mediaflux user authentication deatils.
    --mf.token <token>                   The Mediaflux secure identity token.
    --mf.sid <sid>                       The Mediaflux session id.
    --pid <project-cid>                  The DaRIS project cid.
    --patient.id.map <paitent-id-map>    The file contains AccessionNumber -> PatientID mapping.

Switches:
    --csum                               Generate and compare MD5 checksums of PixelData.
    --continue-on-error                  Continue to upload remaining input files when error occurs.
    --logging                            Enable logging. Log file will be in directory: /Users/wliu5/Documents/workspace5/daris-lifepool-client/target/daris-lifepool-client-0.1.6.
    --verbose                            Show detailed progress information.
    --help                               Display help information.
```


### 1.2. daris-lifepool-data-list
The command line utility to list the images that belongs to the give patients or accessions.

  * *Usage:*
```
Usage: daris-lifepool-data-list [--help] --mf.host <host> --mf.port <port> --mf.transport <transport> [--mf.token <token>|--mf.auth <domain,user,password>|--mf.sid <sid>] --pid <project-cid> [--patient-id <patient-ids>] [accession numbers]

Options:
    --mf.host <host>                     The Mediaflux server host.
    --mf.port <port>                     The Mediaflux server port.
    --mf.transport <transport>           The Mediaflux server transport, can be http, https or tcp/ip.
    --mf.auth <domain,user,password>     The Mediaflux user authentication deatils.
    --mf.token <token>                   The Mediaflux secure identity token.
    --mf.sid <sid>                       The Mediaflux session id.
    --pid <project-cid>                  The DaRIS project cid.
    --patient-id <patient-ids>           One or more patient ids, separated with commas to select the images.

Switches:
    --help                               Display help information.

Arguments:
    [accession numbers]                  One or more accession numbers to select the images.
```

### 1.3. daris-lifepool-data-count

The command line utility to show statistics of the lifepool project: number of images, number of accessions, number of patients and total storage usage(NOTE: total storage usage is less than the total image file size because the data are compressed).

  * *Usage:*
```
Usage: daris-lifepool-data-count [--help] --mf.host <host> --mf.port <port> --mf.transport <transport> [--mf.token <token>|--mf.auth <domain,user,password>|--mf.sid <sid>] --pid <project-cid>

Options:
    --mf.host <host>                     The Mediaflux server host.
    --mf.port <port>                     The Mediaflux server port.
    --mf.transport <transport>           The Mediaflux server transport, can be http, https or tcp/ip.
    --mf.auth <domain,user,password>     The Mediaflux user authentication deatils.
    --mf.token <token>                   The Mediaflux secure identity token.
    --mf.sid <sid>                       The Mediaflux session id.
    --pid <project-cid>                  The DaRIS project cid.

Switches:
    --help                               Display help information.
```
## III. Configuration File



