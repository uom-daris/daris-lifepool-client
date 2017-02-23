package daris.lifepool.client.dicom;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.Map;

import com.pixelmed.dicom.Attribute;
import com.pixelmed.dicom.AttributeList;
import com.pixelmed.dicom.AttributeTag;
import com.pixelmed.dicom.DicomException;
import com.pixelmed.dicom.FileMetaInformation;
import com.pixelmed.dicom.SetOfDicomFiles;
import com.pixelmed.dicom.TagFromName;
import com.pixelmed.dicom.TransferSyntax;

import daris.lifepool.client.App;

public class DicomModify {

    public static void modify(File dicomFile, Map<AttributeTag, String> attributeValues, File of) throws Throwable {
        OutputStream os = new BufferedOutputStream(new FileOutputStream(of));
        try {
            modify(dicomFile, attributeValues, os);
        } finally {
            os.close();
        }
    }

    public static void modify(File dicomFile, Map<AttributeTag, String> attributeValues, OutputStream os)
            throws Throwable {

        AttributeList list = new AttributeList();
        list.read(dicomFile);

        // Put the new tag in place
        if (attributeValues != null) {
            for (AttributeTag tag : attributeValues.keySet()) {
                String value = attributeValues.get(tag);
                putAttribute(list, tag, value);
            }
        }

        save(list, os);

    }

    public static SetOfDicomFiles modify(SetOfDicomFiles dicomFiles, Map<AttributeTag, String> attributeValues,
            File outDir) throws Throwable {
        if (dicomFiles == null || dicomFiles.isEmpty()) {
            return null;
        }
        SetOfDicomFiles outDicomFiles = new SetOfDicomFiles();
        int i = 1;
        for (SetOfDicomFiles.DicomFile dicomFile : dicomFiles) {
            String outFileName = String.format("%08d_%s.dcm", i, Paths.get(dicomFile.getFileName()).getFileName());
            File outFile = new File(outDir, outFileName);
            modify(new File(dicomFile.getFileName()), attributeValues, outFile);
            outDicomFiles.add(outFile);
            i++;
        }
        if (outDicomFiles.isEmpty()) {
            return null;
        }
        return outDicomFiles;
    }

    public static void save(AttributeList list, File of) throws Throwable {

        OutputStream os = new BufferedOutputStream(new FileOutputStream(of));
        try {
            save(list, os);
        } finally {
            os.close();
        }
    }

    public static void save(AttributeList list, OutputStream os) throws Throwable {

        Attribute mediaStorageSOPClassUIDAttr = list.get(TagFromName.MediaStorageSOPClassUID);
        String mediaStorageSOPClassUID = null;
        if (mediaStorageSOPClassUIDAttr != null) {
            mediaStorageSOPClassUID = mediaStorageSOPClassUIDAttr.getSingleStringValueOrNull();
        }
        Attribute mediaStorageSOPInstanceUIDAttr = list.get(TagFromName.MediaStorageSOPInstanceUID);
        String mediaStorageSOPInstanceUID = null;
        if (mediaStorageSOPInstanceUIDAttr != null) {
            mediaStorageSOPInstanceUID = mediaStorageSOPInstanceUIDAttr.getSingleStringValueOrNull();
        }

        /*
         * Cleanup
         */
        list.removeGroupLengthAttributes();
        list.removeMetaInformationHeaderAttributes();
        list.remove(TagFromName.DataSetTrailingPadding);
        list.correctDecompressedImagePixelModule();
        list.insertLossyImageCompressionHistoryIfDecompressed();

        if (mediaStorageSOPClassUID != null && mediaStorageSOPInstanceUID != null) {
            FileMetaInformation.addFileMetaInformation(list, mediaStorageSOPClassUID, mediaStorageSOPInstanceUID,
                    TransferSyntax.ExplicitVRLittleEndian, App.DICOM_AE_TITLE);
        } else {
            FileMetaInformation.addFileMetaInformation(list, TransferSyntax.ExplicitVRLittleEndian, App.DICOM_AE_TITLE);
        }

        list.write(os, TransferSyntax.ExplicitVRLittleEndian, true, true);
    }

    public static void putAttribute(AttributeList list, AttributeTag tag, String value) throws DicomException {

        Attribute attr = list.get(tag);
        if (attr != null) {
            attr.setValue(value);
        } else {
            list.putNewAttribute(tag).addValue(value);
        }
    }

}
