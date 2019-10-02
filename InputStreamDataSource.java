package amanda.edms;

import javax.activation.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Custom data source to pass InputStream to OpenText API to send files to OpenText server.
 */
public class InputStreamDataSource implements DataSource {

    private InputStream inputStream;

    private String name;

    public InputStreamDataSource(String name, InputStream inputStream) {
        this.name = name;
        this.inputStream = inputStream;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return inputStream;
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String getContentType() {
        return "*/*";
    }

    @Override
    public String getName() {
        return name;
    }

}
