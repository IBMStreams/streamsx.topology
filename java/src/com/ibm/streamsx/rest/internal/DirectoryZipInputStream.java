package com.ibm.streamsx.rest.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;


/**
 * Given a path to a directory, this class creates an InputStream containing
 * the contents of the directory, zipped.  Empty directories are included.
 */
public class DirectoryZipInputStream {

  /**
   * Create an InputStream from a directory, containing the contents of
   * the directory, zipped.
   */
  static public InputStream fromPath(Path root) throws IOException {
    InputStream is;
    try (ZipStreamVisitor visitor = new ZipStreamVisitor()) {
      Files.walkFileTree(root, visitor);
      visitor.close();
      is = visitor.getInputStream();
    }
    return is;
  }

  // We want to write a bunch of bytes to a ByteArrayOutputStream, then
  // create a ByteArrayInputStream from the bytes written to the outout stream.
  // Problem is, ByteArrayOutputStream does not provide a good way to do this.
  // ByteArrayOutputStream.toByteArray() creates a copy of the byte array,
  // but we just want to take ownership of the byte array.  After calling
  // getInputStream(), the output stream probably should not be used.
  static private class ByteArrayOutputStreamWithInputStream extends ByteArrayOutputStream {
    public InputStream getInputStream() {
      InputStream is = new ByteArrayInputStream(this.buf,0,this.count);
      this.buf = new byte[0];
      this.count = 0;
      return is;
    }
  }

  static private class ZipStreamVisitor extends SimpleFileVisitor<Path> implements Closeable {
    private ByteArrayOutputStreamWithInputStream os;
    private ZipOutputStream zos;
    private Path root;

    public ZipStreamVisitor() {
      os = new ByteArrayOutputStreamWithInputStream();
      zos = new ZipOutputStream(os);
    }

    public InputStream getInputStream() {
      return os.getInputStream();
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
      // If an entry has a name ending with '/', ZipOutputStream treats
      // it as a directory.
      ZipEntry entry = new ZipEntry(relativePath(dir) + '/');
      zos.putNextEntry(entry);
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
      try (InputStream fis = Files.newInputStream(file)) {
	ZipEntry entry = new ZipEntry(relativePath(file));
        zos.putNextEntry(entry);
        copy(fis, zos);
      }
      return FileVisitResult.CONTINUE;
    }

    public void close() throws IOException {
      zos.close();
    }

    private String relativePath(Path path) {
      if (root == null) {
        root = path.getParent();
      }
      return root.relativize(path).toString();
    }

    static private void copy(InputStream is, OutputStream os) throws IOException {
      byte[] block = new byte[8192];
      int count = 0;
      while (count != -1) {
        os.write(block, 0, count);
        count = is.read(block);
      }
    }
  }
}
