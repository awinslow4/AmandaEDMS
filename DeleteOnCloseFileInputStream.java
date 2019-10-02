package amanda.edms;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * This InputStream deletes the temp file when the input stream closed by AMANDA application.
 */
public class DeleteOnCloseFileInputStream extends FileInputStream {

    private File file;

    public DeleteOnCloseFileInputStream(String fileName) throws FileNotFoundException {
        this(new File(fileName));
    }

    public DeleteOnCloseFileInputStream(File file) throws FileNotFoundException {
        super(file);
        this.file = file;
    }

    public void close() throws IOException {
        try {
            super.close();
        } finally {
            if (file != null) {
                file.delete();
                file = null;
            }
        }
    }
}
